// Run with node tests/fixtures/generate_bingx_trades.cjs [original-repo].
// Uses only original pure helpers; never loads exchange clients or environment files.
const fs = require('fs'), path = require('path'), crypto = require('crypto');
const root = path.resolve(process.argv[2] || '../satoshi-channel-updates-manager');
const ts = require(path.join(root, 'node_modules/typescript'));
require.extensions['.ts'] = (mod, filename) => mod._compile(ts.transpileModule(fs.readFileSync(filename, 'utf8'), {compilerOptions:{module:ts.ModuleKind.CommonJS,target:ts.ScriptTarget.ES2020}}).outputText, filename);
const {CreateTradeObject} = require(path.join(root,'src/commands/create-trade-object.ts'));
const t = require(path.join(root,'src/targets/targetsMethods.ts'));
const {Precisions} = require(path.join(root,'src/targets/Precision.ts'));
const sizes = require(path.join(root,'src/trade-processing/position-size.ts'));
const {projectBingXExecutionTrade} = require(path.join(root,'src/hedge-mode/bingx-trade-projection.ts'));
const precision = new Precisions();
function job(symbol, price, buy, sell, stop, isLong=true) {
 const j=JSON.parse(fs.readFileSync(path.join(__dirname,'admission-job.json')));
 j.symbol=symbol; Object.assign(j.signalData,{symbol,buy_targets:buy,sell_targets:sell,stop_loss:stop,is_long:isLong,leverage:'3x'});
 Object.assign(j.channelSettings,{id:-100,name:'Fixture channel',default_quantity:.1,default_sell_targets:[{fraction:.33},{fraction:.33},{fraction:.34}],xPro:null});
 j.client.name='Fixture account';
 j.idempotencyKey=`auto-trade:-100:7:42:account-1:BingX:futures:${symbol}`; j.partitionKey=`client-symbol:account-1:futures:${symbol}`;
 j.marketData.bingXFutures={currPrice:price,symbolInfo:{symbol:symbol.replace(/USDT$/,'-USDT'),status:1,tickSize:'0.0001',lotSize:'0.1',minQty:'0.1',minNotional:5}};
 return j;
}
const ada = () => job('ADAUSDT','0.2570',['0.2570'],['0.26','0.27','0.28'],'0.18');
const cases=[{name:'ada_long',job:ada(),balance:100,route:'HEDGE_V1'},
{name:'ada_short',job:job('ADAUSDT','0.2570',['0.2570'],['0.25','0.24','0.23'],'0.28',false),balance:100,route:'ONEWAY_LEGACY'},
{name:'breakout_range',job:job('ADAUSDT','0.2570',['0.25','0.26'],['0.28','0.29','0.30'],'0.18'),balance:500,route:'HEDGE_V1'},
{name:'static_fallback',job:ada(),balance:20,route:'HEDGE_V1'},
{name:'signal_override',job:ada(),balance:1000,route:'HEDGE_V1'},
{name:'reduced_targets',job:ada(),balance:55,route:'HEDGE_V1'},
{name:'integer_shorthand',job:job('ADAUSDT','0.2570',['2570'],['2600','2700','2800'],'1800'),balance:100,route:'HEDGE_V1'}];
cases[2].job.signalData.breakOutEntry=true;
cases[2].job.channelSettings.default_buy_targets=[{fraction:.3},{fraction:.3},{fraction:.4}];
cases[3].job.channelSettings.position_size_mode='static'; cases[3].job.channelSettings.static_quote_amount=100;
cases[4].job.signalData.position=.02;
for (const isLong of [true,false]) {
 const j=job('BTCUSDT','62000.00',['62000'],isLong?['63000','64000','65000']:['61000','60000','59000'],isLong?'61000':'63000',isLong);
 Object.assign(j.marketData.bingXFutures.symbolInfo,{tickSize:'0.01',lotSize:'0.001',minQty:'0.001'});
 cases.push({name:isLong?'btc_long':'btc_short',job:j,balance:1000,route:'HEDGE_V1'});
}
const range=job('SOLUSDT','71.50',['71','72'],['88','90','92','94'],'70');
Object.assign(range.marketData.bingXFutures.symbolInfo,{tickSize:'0.01',lotSize:'0.01',minQty:'0.01'});
range.channelSettings.default_buy_targets=[{fraction:'0.5'},{fraction:'0.5'}];
range.channelSettings.default_sell_targets=[{fraction:'0.4'},{fraction:'0.6'}];
cases.push({name:'sol_two_entries_string_fractions',job:range,balance:1000,route:'HEDGE_V1'});
const pro=ada(); pro.channelSettings.xPro='0';
cases.push({name:'string_pro_setting',job:pro,balance:100,route:'HEDGE_V1'});
const smallPro=ada();smallPro.channelSettings.xPro=1;smallPro.signalData.sell_targets=['0.26'];
cases.push({name:'small_pro_single_target',job:smallPro,balance:40,route:'HEDGE_V1'});
(async()=>{
 for(const c of cases){
  const j=c.job,s=j.signalData,config=j.channelSettings,{currPrice,symbolInfo}=j.marketData.bingXFutures;
  const sizing=sizes.calculatePositionSize({positionSize:sizes.resolvePositionSize(s,config,'futures').positionSize,market:'futures',availableBalance:c.balance,currentPrice:currPrice,leverage:3}).calculation;
  let trade=await new CreateTradeObject(config.strategy,config.xPro,symbolInfo.symbol,42,sizing.rawTradeQuantity,s.is_long,symbolInfo,currPrice,'_binance_futures_').execute();
  trade.buy_targets=t.def_buy_targets(s,config,currPrice,sizing.rawTradeQuantity).map(e=>t.createEntryTarget(e.price,e.quantity,e.type,e.fraction,trade.precisions,currPrice,s.is_long));
  if(trade.buy_targets.some(x=>x.err)) throw Error(JSON.stringify(trade.buy_targets));
  const exec=sizes.reconcileExecutablePositionSize(sizing,trade.buy_targets); if(!exec.valid)throw Error(exec.satoshi_err);
  trade.quantity=exec.executableTradeQuantity; trade.used_coins.wished_quantity=trade.quantity;trade.positionSizing=exec.metadata;
  const min=precision.min_trade_quantity(s.sell_targets.length,s.buy_targets.length,trade.precisions,trade.strategy.is_pro);
  let profitConfig=config;
  if(trade.quantity<min.min_qty||exec.executableNotionalQuoteAmount<min.min_not){
   const possible=t.check_possible_sell_targets(s.sell_targets,s.buy_targets,trade.quantity,exec.executableNotionalQuoteAmount,trade);
   const optimized=precision.min_trade_quantity(possible.length,s.buy_targets.length,trade.precisions,trade.strategy.is_pro);
   if(trade.quantity<optimized.min_qty||exec.executableNotionalQuoteAmount<optimized.min_not)throw Error('insufficient fixture');profitConfig={default_sell_targets:possible};
  }
  trade.sell_targets=t.def_sell_targets(s.sell_targets,profitConfig).map(e=>t.createProfitTarget(e.price,e.fraction,trade.quantity,trade.precisions,currPrice,s.is_long,trade.symbol,trade.buy_targets,3));
  trade.stop_loss=t.createStopLoss(precision.binance_precision_price(s.stop_loss,currPrice),currPrice,trade.quantity,trade.precisions,s.is_long,trade.symbol,trade.buy_targets);
  if(trade.sell_targets.some(x=>x.err)||trade.stop_loss.err)throw Error(JSON.stringify(trade));
  trade.auto_Trade={channel_id:config.id,channelName:config.name,message_id:j.messageId,usingChannelConfig:true,exchangeClientName:j.client.name};
  trade.tradeLeverage=s.leverage;trade.futuresMarginMode='isolated';trade=projectBingXExecutionTrade(trade,c.route);
  trade.id=crypto.createHash('sha256').update(j.idempotencyKey).digest('hex').slice(0,32); trade.idempotencyKey=j.idempotencyKey;
  trade.routing={jobType:'create_trade',provider:'BingX',market:'futures',symbol:j.symbol,partitionKey:j.partitionKey};
  for(const target of [...trade.buy_targets,...trade.sell_targets,trade.stop_loss])delete target.clientOrderId;
  c.expected={expires_at:j.tradeExpiresAt,trade_object:trade,client_data:j.client};
 }
 fs.writeFileSync(path.join(__dirname,'bingx-trades.json'),JSON.stringify(cases,null,2)+'\n');
})();
