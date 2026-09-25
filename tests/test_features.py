from datetime import datetime, timedelta, timezone
from decimal import Decimal
from uuid import uuid4
from integrity_engine.features.trades import trade_features
from integrity_engine.features.orderbook import orderbook_features
from integrity_engine.features.windows import EventWindow
from integrity_engine.schemas.trade import TradeEvent
from integrity_engine.schemas.orderbook import OrderBookSnapshot, OrderBookLevel

T=datetime(2026,1,1,tzinfo=timezone.utc)
def trade(price,qty,side,seconds=0):
 return TradeEvent(schema_version='1',event_id=uuid4(),event_type='trade',exchange='binance',symbol='BTC-USDT',exchange_timestamp=T+timedelta(seconds=seconds),received_timestamp=T,price=Decimal(price),quantity=Decimal(qty),side=side,quote_quantity=Decimal(price)*Decimal(qty))
def book(seconds=0):
 return OrderBookSnapshot(schema_version='1',event_id=uuid4(),exchange_timestamp=T+timedelta(seconds=seconds),received_timestamp=T,sequence_id=seconds,best_bid_price=Decimal('99'),best_bid_quantity=Decimal('2'),best_ask_price=Decimal('101'),best_ask_quantity=Decimal('3'),spread=Decimal('2'),mid_price=Decimal('100'),bids=[OrderBookLevel(price=Decimal('99'),quantity=Decimal('2')),OrderBookLevel(price=Decimal('98'),quantity=Decimal('1'))],asks=[OrderBookLevel(price=Decimal('101'),quantity=Decimal('3')),OrderBookLevel(price=Decimal('102'),quantity=Decimal('1'))])
def test_trade_features():
 f=trade_features([trade('100','2','buy'),trade('110','1','sell',1)])
 assert f['price_return']==Decimal('0.1');assert f['base_volume']==3;assert f['buy_volume']==2;assert f['sell_volume']==1;assert f['vwap']==Decimal('310')/3;assert f['largest_trade_size']==2
 assert f['trade_imbalance']==Decimal(1)/3
def test_orderbook_features():
 f=orderbook_features([book()]);assert f['spread']==2;assert f['spread_bps']==200;assert f['bid_depth_1']==2;assert f['ask_depth_1']==3;assert f['orderbook_imbalance_1']==Decimal(-1)/5
def test_zero_denominator():
 assert trade_features([])=={}; assert orderbook_features([])=={}
def test_window_expiration_and_bound():
 w=EventWindow(timedelta(seconds=10));w.add(T,'a');w.add(T+timedelta(seconds=20),'b');assert list(w.values(T+timedelta(seconds=20),T+timedelta(seconds=20)))==['b'];assert len(w)==1
def test_multiple_windows_and_serialization():
 f=trade_features([trade('100','1','buy')]);assert f['price_range']==0
 b=book();assert b.model_dump_json();assert len(b.bids)==2

def test_feature_engine_calculates_all_windows():
 from integrity_engine.features.engine import FeatureEngine
 engine = FeatureEngine(None)
 engine.add_trade(trade('100', '2', 'buy'))
 engine.add_book(book())
 metrics = engine.calculate(T + timedelta(seconds=1))
 assert {m.window for m in metrics} == {'10s', '30s', '1m', '5m'}
 assert all(m.trade_event_count == 1 and m.orderbook_event_count == 1 for m in metrics)
 assert all(m.data_complete and m.base_volume == Decimal('2') for m in metrics)
