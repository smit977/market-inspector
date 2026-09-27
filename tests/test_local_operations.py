"""Exercise operational failure paths without touching a real Docker daemon."""
import os
from pathlib import Path
import shutil
import subprocess

import pytest


@pytest.fixture
def stack(tmp_path):
    root = Path(__file__).resolve().parents[1]
    scripts = tmp_path / 'scripts'
    scripts.mkdir()
    for name in ('bootstrap_env.sh', 'stack_common.sh', 'start_stack.sh', 'stop_stack.sh', 'status_stack.sh'):
        shutil.copy(root / 'scripts' / name, scripts / name)
    (tmp_path / '.env').write_text('TEST_SECRET=not-to-be-printed\n')
    bindir = tmp_path / 'bin'; bindir.mkdir()
    openssl = bindir / 'openssl'
    openssl.write_text('#!/usr/bin/env bash\nprintf "%s\\n" "generated-test-secret-marker"\n')
    openssl.chmod(0o755)
    docker = bindir / 'docker'
    docker.write_text('''#!/usr/bin/env bash
printf '%s\\n' "$*" >> "$CALLS"
case "$*" in
 *' up '*) exit "${UP_EXIT:-0}";;
 *'ps --all --quiet'*) echo container-id;;
 inspect*)
   while IFS= read -r service; do
     state="running"; health="none"; restarts=0
     if [[ "${CONTAINER_STATE:-}" == exited* ]]; then state=exited; restarts=2; fi
     printf '{"Config":{"Labels":{"com.docker.compose.service":"%s"}},"State":{"Status":"%s"},"RestartCount":%s}\n' "$service" "$state" "$restarts"
   done <<< "$LIVE_SERVICE_LIST";;
esac
''')
    docker.chmod(0o755)
    env = dict(os.environ, PATH=f'{bindir}:{os.environ["PATH"]}', CALLS=str(tmp_path/'calls'), COMPOSE_PROFILES='replay,fabric')
    return tmp_path, env


def run(stack, script, **env):
    root, base = stack
    result = subprocess.run(['bash', str(root/'scripts'/script)], env=dict(base, **env), capture_output=True, text=True)
    calls = (root/'calls').read_text() if (root/'calls').exists() else ''
    assert 'not-to-be-printed' not in result.stdout + result.stderr + calls
    return result, calls


def test_start_selects_live_services_and_waits(stack):
    result, calls = run(stack, 'start_stack.sh')
    assert result.returncode == 0
    up = next(line for line in calls.splitlines() if ' up ' in line)
    assert '--detach --wait --wait-timeout' in up
    assert 'integrity-engine' in up and 'derivatives-features' in up
    assert 'replay' not in up and 'fabric' not in up


def test_bootstrap_generates_private_local_credentials(stack):
    root, _ = stack
    (root/'.env').unlink()
    (root/'.env.example').write_text(
        'GRAFANA_ADMIN_PASSWORD=\nCLICKHOUSE_GRAFANA_PASSWORD=\nCLICKHOUSE_MCP_PASSWORD=\n'
    )
    result, _ = run(stack, 'bootstrap_env.sh')
    env_text = (root/'.env').read_text()
    assert result.returncode == 0
    assert 'generated-test-secret-marker' not in result.stdout + result.stderr
    assert env_text.count('=generated-test-secret-marker') == 3
    assert (root/'.env').stat().st_mode & 0o777 == 0o600


def test_start_does_not_claim_success_after_failed_health_wait(stack):
    result, _ = run(stack, 'start_stack.sh', UP_EXIT='1')
    assert result.returncode != 0
    assert 'Live stack started' not in result.stdout


def test_stop_preserves_volumes(stack):
    result, calls = run(stack, 'stop_stack.sh')
    assert result.returncode == 0
    assert 'stop --timeout 60' in calls
    assert 'down' not in calls and ' -v' not in calls


def test_status_detects_failed_worker(stack):
    result, _ = run(stack, 'status_stack.sh', CONTAINER_STATE='exited no-healthcheck restarts=2')
    assert result.returncode != 0


def test_missing_env_fails_before_docker(stack):
    (stack[0]/'.env').unlink()
    result, calls = run(stack, 'start_stack.sh')
    assert result.returncode != 0
    assert not calls
