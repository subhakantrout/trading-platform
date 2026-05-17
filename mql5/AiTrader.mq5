//+------------------------------------------------------------------+
//|                                               AiTrader.Pro.mq5   |
//|                                     AI Quant Terminal v5 Engine  |
//+------------------------------------------------------------------+
#property copyright "AI Quant Terminal"
#property version   "5.00"
#property strict
#property description "Revolutionary AI-Powered Trading Terminal"
#property description "Streams ALL market data to AI backend"

//+------------------------------------------------------------------+
//| Input Parameters                                                 |
//+------------------------------------------------------------------+
input string   ServerURL             = "http://127.0.0.1:8000/mt5/update";
input int      UpdateIntervalMs      = 1000;
input int      ExtendedIntervalSec   = 30;
input int      MaxBarsToSend         = 500;
input bool     EnableAutoTrading     = true;
input string   SignalCheckURL        = "http://127.0.0.1:8000/api/signals/pending";
input string   DrawingCheckURL       = "http://127.0.0.1:8000/api/mt5/drawings";
input string   LogEventURL           = "http://127.0.0.1:8000/api/system/log";
input int      SignalCheckIntervalSec= 60;
input double   DefaultLotSize        = 0.01;
input double   RiskPerTrade          = 2.0;
input int      MagicNumber           = 20240516;
input bool     EnableLogging         = true;

input string   CorrSymbol1           = "EURUSD";
input string   CorrSymbol2           = "XAUUSD";
input string   CorrSymbol3           = "US30";
input string   CorrSymbol4           = "NAS100";
input string   CorrSymbol5           = "";

//+------------------------------------------------------------------+
//| Global Variables                                                 |
//+------------------------------------------------------------------+
int h_rsi, h_sma20, h_ema20, h_ema50, h_ema200, h_atr, h_macd, h_bb, h_stoch;
int h_adx, h_obv, h_vol, h_cci, h_wr, h_momentum, h_force, h_bears, h_bulls;
int h_ao, h_ac, h_stddev, h_dem, h_rvi, h_mfi, h_ichimoku, h_sar, h_fractals;
int h_gator, h_volumes_real;

int h_mtf_rsi[8], h_mtf_sma[8], h_mtf_atr[8], h_mtf_macd[8];
ENUM_TIMEFRAMES mtf_periods[8] = {PERIOD_M1, PERIOD_M5, PERIOD_M15, PERIOD_M30, PERIOD_H1, PERIOD_H4, PERIOD_D1, PERIOD_W1};
string mtf_names[8] = {"M1", "M5", "M15", "M30", "H1", "H4", "D1", "W1"};

bool is_sending = false;
datetime lastSignalCheck = 0;
int signalCheckIntervalTicks = 1;
int extendedIntervalTicks = 1;
int tickCounter = 0;

string corr_symbols[5];
int corr_count = 0;

//+------------------------------------------------------------------+
//| Expert Initialization                                            |
//+------------------------------------------------------------------+
int OnInit()
{
   h_rsi       = iRSI(_Symbol, _Period, 14, PRICE_CLOSE);
   h_sma20     = iMA(_Symbol, _Period, 20, 0, MODE_SMA, PRICE_CLOSE);
   h_ema20     = iMA(_Symbol, _Period, 20, 0, MODE_EMA, PRICE_CLOSE);
   h_ema50     = iMA(_Symbol, _Period, 50, 0, MODE_EMA, PRICE_CLOSE);
   h_ema200    = iMA(_Symbol, _Period, 200, 0, MODE_EMA, PRICE_CLOSE);
   h_atr       = iATR(_Symbol, _Period, 14);
   h_macd      = iMACD(_Symbol, _Period, 12, 26, 9, PRICE_CLOSE);
   h_bb        = iBands(_Symbol, _Period, 20, 0, 2.0, PRICE_CLOSE);
   h_stoch     = iStochastic(_Symbol, _Period, 5, 3, 3, MODE_SMA, STO_LOWHIGH);
   h_adx       = iADX(_Symbol, _Period, 14);
   h_obv       = iOBV(_Symbol, _Period, VOLUME_TICK);
   h_vol       = iVolumes(_Symbol, _Period, VOLUME_TICK);
   h_volumes_real = iVolumes(_Symbol, _Period, VOLUME_REAL);
   h_cci       = iCCI(_Symbol, _Period, 14, PRICE_TYPICAL);
   h_wr        = iWPR(_Symbol, _Period, 14);
   h_momentum  = iMomentum(_Symbol, _Period, 10, PRICE_CLOSE);
   h_force     = iForce(_Symbol, _Period, 13, MODE_SMA, VOLUME_TICK);
   h_bears     = iBearsPower(_Symbol, _Period, 13);
   h_bulls     = iBullsPower(_Symbol, _Period, 13);
   h_ao        = iAO(_Symbol, _Period);
   h_ac        = iAC(_Symbol, _Period);
   h_stddev    = iStdDev(_Symbol, _Period, 20, 0, MODE_SMA, PRICE_CLOSE);
   h_dem       = iDeMarker(_Symbol, _Period, 14);
   h_rvi       = iRVI(_Symbol, _Period, 10);
   h_mfi       = iMFI(_Symbol, _Period, 14, VOLUME_TICK);
   h_ichimoku  = iIchimoku(_Symbol, _Period, 9, 26, 52);
   h_sar       = iSAR(_Symbol, _Period, 0.02, 0.2);
   h_fractals  = iFractals(_Symbol, _Period);
   h_gator     = iGator(_Symbol, _Period, 13, 8, 8, 5, 5, 3, MODE_SMA, PRICE_CLOSE);

   for(int i = 0; i < 8; i++)
   {
      h_mtf_rsi[i]  = iRSI(_Symbol, mtf_periods[i], 14, PRICE_CLOSE);
      h_mtf_sma[i]  = iMA(_Symbol, mtf_periods[i], 20, 0, MODE_SMA, PRICE_CLOSE);
      h_mtf_atr[i]  = iATR(_Symbol, mtf_periods[i], 14);
      h_mtf_macd[i] = iMACD(_Symbol, mtf_periods[i], 12, 26, 9, PRICE_CLOSE);
   }

   corr_count = 0;
   if(CorrSymbol1 != "") { corr_symbols[corr_count] = CorrSymbol1; corr_count++; }
   if(CorrSymbol2 != "") { corr_symbols[corr_count] = CorrSymbol2; corr_count++; }
   if(CorrSymbol3 != "") { corr_symbols[corr_count] = CorrSymbol3; corr_count++; }
   if(CorrSymbol4 != "") { corr_symbols[corr_count] = CorrSymbol4; corr_count++; }
   if(CorrSymbol5 != "") { corr_symbols[corr_count] = CorrSymbol5; corr_count++; }

   int safeIntervalMs = UpdateIntervalMs <= 0 ? 1000 : UpdateIntervalMs;
   signalCheckIntervalTicks = (int)((double)SignalCheckIntervalSec * 1000.0 / (double)safeIntervalMs);
   if(signalCheckIntervalTicks < 1) signalCheckIntervalTicks = 1;
   extendedIntervalTicks = (int)((double)ExtendedIntervalSec * 1000.0 / (double)safeIntervalMs);
   if(extendedIntervalTicks < 1) extendedIntervalTicks = 1;

   ObjectsDeleteAll(0, "AI_");
   EventSetMillisecondTimer(safeIntervalMs);
   
   Print("AI Trader v5: Initialized on ", _Symbol, " ", EnumToString(_Period));
   Print("AI Trader: Server: ", ServerURL, " AutoTrade: ", EnableAutoTrading, " Correlations: ", corr_count);
   return(INIT_SUCCEEDED);
}

//+------------------------------------------------------------------+
//| Expert Deinitialization                                          |
//+------------------------------------------------------------------+
void OnDeinit(const int reason)
{
   EventKillTimer();
   
   int handles[] = {h_rsi, h_sma20, h_ema20, h_ema50, h_ema200, h_atr, h_macd, h_bb, h_stoch, h_adx, h_obv, h_vol, h_volumes_real, h_cci, h_wr, h_momentum, h_force, h_bears, h_bulls, h_ao, h_ac, h_stddev, h_dem, h_rvi, h_mfi, h_ichimoku, h_sar, h_fractals, h_gator};
   for(int i = 0; i < ArraySize(handles); i++)
   {
      if(handles[i] != INVALID_HANDLE)
         IndicatorRelease(handles[i]);
   }
   
   for(int i = 0; i < 8; i++)
   {
      if(h_mtf_rsi[i] != INVALID_HANDLE) IndicatorRelease(h_mtf_rsi[i]);
      if(h_mtf_sma[i] != INVALID_HANDLE) IndicatorRelease(h_mtf_sma[i]);
      if(h_mtf_atr[i] != INVALID_HANDLE) IndicatorRelease(h_mtf_atr[i]);
      if(h_mtf_macd[i] != INVALID_HANDLE) IndicatorRelease(h_mtf_macd[i]);
   }
   
   Print("AI Trader v5: Shutdown. Reason: ", reason);
}

//+------------------------------------------------------------------+
//| Timer Event                                                      |
//+------------------------------------------------------------------+
void OnTimer()
{
   if(is_sending) return;

   tickCounter++;
   SendDataToServer();
   CheckForDrawings();

   if(tickCounter % extendedIntervalTicks == 0)
   {
      SendExtendedData();
   }

   if(EnableAutoTrading && tickCounter >= signalCheckIntervalTicks)
   {
      tickCounter = 0;
      CheckForSignals();
   }
}

//+------------------------------------------------------------------+
//| Safe Double to String                                            |
//+------------------------------------------------------------------+
string DblStr(double val, int digits)
{
   if(val == EMPTY_VALUE || !MathIsValidNumber(val))
      return "null";
   return DoubleToString(val, digits);
}

//+------------------------------------------------------------------+
//| Safe Indicator Value                                             |
//+------------------------------------------------------------------+
string GetInd(int handle, int buffer, int shift, int digits)
{
   double val[1];
   if(handle != INVALID_HANDLE && CopyBuffer(handle, buffer, shift, 1, val) > 0)
      return DblStr(val[0], digits);
   return "null";
}

//+------------------------------------------------------------------+
//| Send Core Market Data                                            |
//+------------------------------------------------------------------+
void SendDataToServer()
{
   double bid = SymbolInfoDouble(_Symbol, SYMBOL_BID);
   double ask = SymbolInfoDouble(_Symbol, SYMBOL_ASK);
   if(!MathIsValidNumber(bid) || !MathIsValidNumber(ask) || bid <= 0) return;

   is_sending = true;

   double spread_val = MathAbs(ask - bid) / (_Point > 0 ? _Point : 1e-6);
   double balance = AccountInfoDouble(ACCOUNT_BALANCE);
   double equity = AccountInfoDouble(ACCOUNT_EQUITY);
   double margin_free = AccountInfoDouble(ACCOUNT_MARGIN_FREE);
   double margin = AccountInfoDouble(ACCOUNT_MARGIN);
   double margin_level = AccountInfoDouble(ACCOUNT_MARGIN_LEVEL);

   int positions_total = PositionsTotal();
   double total_profit = 0;
   int buy_count = 0, sell_count = 0;
   double buy_volume = 0, sell_volume = 0;
   
   for(int i = positions_total - 1; i >= 0; i--)
   {
      ulong ticket = PositionGetTicket(i);
      if(ticket > 0 && PositionSelectByTicket(ticket))
      {
         total_profit += PositionGetDouble(POSITION_PROFIT);
         if(PositionGetInteger(POSITION_TYPE) == POSITION_TYPE_BUY)
         {
            buy_count++;
            buy_volume += PositionGetDouble(POSITION_VOLUME);
         }
         else
         {
            sell_count++;
            sell_volume += PositionGetDouble(POSITION_VOLUME);
         }
      }
   }

   string ind = "{";
   ind += "\"rsi_14\":" + GetInd(h_rsi, 0, 0, 2) + ",";
   ind += "\"sma_20\":" + GetInd(h_sma20, 0, 0, _Digits) + ",";
   ind += "\"ema_20\":" + GetInd(h_ema20, 0, 0, _Digits) + ",";
   ind += "\"ema_50\":" + GetInd(h_ema50, 0, 0, _Digits) + ",";
   ind += "\"ema_200\":" + GetInd(h_ema200, 0, 0, _Digits) + ",";
   ind += "\"atr_14\":" + GetInd(h_atr, 0, 0, _Digits) + ",";
   ind += "\"macd_main\":" + GetInd(h_macd, 0, 0, _Digits) + ",";
   ind += "\"macd_sig\":" + GetInd(h_macd, 1, 0, _Digits) + ",";
   
   double macd_buf0[2], macd_buf1[2];
   if(CopyBuffer(h_macd, 0, 0, 2, macd_buf0) >= 2 && CopyBuffer(h_macd, 1, 0, 2, macd_buf1) >= 2)
      ind += "\"macd_hist\":" + DblStr(macd_buf0[0] - macd_buf1[0], _Digits) + ",";
   else
      ind += "\"macd_hist\":null,";
   
   ind += "\"bb_up\":" + GetInd(h_bb, 1, 0, _Digits) + ",";
   ind += "\"bb_low\":" + GetInd(h_bb, 2, 0, _Digits) + ",";
   ind += "\"bb_mid\":" + GetInd(h_bb, 0, 0, _Digits) + ",";
   ind += "\"stoch_k\":" + GetInd(h_stoch, 0, 0, 2) + ",";
   ind += "\"stoch_d\":" + GetInd(h_stoch, 1, 0, 2) + ",";
   ind += "\"adx\":" + GetInd(h_adx, 0, 0, 2) + ",";
   ind += "\"adx_plusdi\":" + GetInd(h_adx, 1, 0, 2) + ",";
   ind += "\"adx_minusdi\":" + GetInd(h_adx, 2, 0, 2) + ",";
   ind += "\"obv\":" + GetInd(h_obv, 0, 0, 0) + ",";
   ind += "\"tick_volume\":" + GetInd(h_vol, 0, 0, 0) + ",";
   ind += "\"cci_14\":" + GetInd(h_cci, 0, 0, 2) + ",";
   ind += "\"williams_r\":" + GetInd(h_wr, 0, 0, 2) + ",";
   ind += "\"momentum_10\":" + GetInd(h_momentum, 0, 0, 2) + ",";
   ind += "\"force_index\":" + GetInd(h_force, 0, 0, 2) + ",";
   ind += "\"bears_power\":" + GetInd(h_bears, 0, 0, _Digits) + ",";
   ind += "\"bulls_power\":" + GetInd(h_bulls, 0, 0, _Digits) + ",";
   ind += "\"awesome_osc\":" + GetInd(h_ao, 0, 0, _Digits) + ",";
   ind += "\"accelerator_osc\":" + GetInd(h_ac, 0, 0, _Digits) + ",";
   ind += "\"std_dev_20\":" + GetInd(h_stddev, 0, 0, _Digits) + ",";
   ind += "\"demarker\":" + GetInd(h_dem, 0, 0, 2) + ",";
   ind += "\"rvi_main\":" + GetInd(h_rvi, 0, 0, 2) + ",";
   ind += "\"rvi_sig\":" + GetInd(h_rvi, 1, 0, 2) + ",";
   ind += "\"mfi_14\":" + GetInd(h_mfi, 0, 0, 2) + ",";
   ind += "\"ichimoku_tenkan\":" + GetInd(h_ichimoku, 0, 0, _Digits) + ",";
   ind += "\"ichimoku_kijun\":" + GetInd(h_ichimoku, 1, 0, _Digits) + ",";
   ind += "\"ichimoku_senkou_a\":" + GetInd(h_ichimoku, 2, 0, _Digits) + ",";
   ind += "\"ichimoku_senkou_b\":" + GetInd(h_ichimoku, 3, 0, _Digits) + ",";
   ind += "\"ichimoku_chikou\":" + GetInd(h_ichimoku, 4, 0, _Digits) + ",";
   ind += "\"sar\":" + GetInd(h_sar, 0, 0, _Digits) + ",";
   ind += "\"fractal_up\":" + GetInd(h_fractals, 0, 0, _Digits) + ",";
   ind += "\"fractal_down\":" + GetInd(h_fractals, 1, 0, _Digits) + ",";
   ind += "\"gator_upper\":" + GetInd(h_gator, 0, 0, _Digits) + ",";
   ind += "\"gator_lower\":" + GetInd(h_gator, 1, 0, _Digits);
   ind += "}";

   MqlRates rates[];
   ArraySetAsSeries(rates, true);
   int count = MathMin(MaxBarsToSend, 5000);
   int bars = iBars(_Symbol, _Period);
   if(count > bars) count = bars;
   int copied = CopyRates(_Symbol, _Period, 0, count, rates);
   if(copied <= 0) { is_sending = false; return; }

   string candles = "[";
   int candles_added = 0;
   for(int i = 0; i < copied; i++)
   {
      string c = "{\"time\":" + IntegerToString((long)rates[i].time);
      c += ",\"open\":" + DoubleToString(rates[i].open, _Digits);
      c += ",\"high\":" + DoubleToString(rates[i].high, _Digits);
      c += ",\"low\":" + DoubleToString(rates[i].low, _Digits);
      c += ",\"close\":" + DoubleToString(rates[i].close, _Digits);
      c += ",\"volume\":" + IntegerToString((long)rates[i].tick_volume);
      c += ",\"real_volume\":" + IntegerToString((long)rates[i].real_volume);
      c += "}";
      
      if(StringLen(candles) + StringLen(c) > 50000) break;
      if(candles_added > 0) candles += ",";
      candles += c;
      candles_added++;
   }
   candles += "]";

   int sr_range = MathMin(copied, 50);
   double highest_50 = 0, lowest_50 = DBL_MAX;
   for(int i = 0; i < sr_range; i++)
   {
      if(rates[i].high > highest_50) highest_50 = rates[i].high;
      if(rates[i].low < lowest_50) lowest_50 = rates[i].low;
   }

   double change = 0;
   if(copied > 1 && rates[1].close > 0)
      change = bid - rates[1].close;

   string json = "{";
   json += "\"symbol\":\"" + _Symbol + "\",";
   json += "\"timeframe\":\"" + EnumToString(_Period) + "\",";
   json += "\"bid\":" + DoubleToString(bid, _Digits) + ",";
   json += "\"ask\":" + DoubleToString(ask, _Digits) + ",";
   json += "\"spread\":" + DoubleToString(spread_val, 1) + ",";
   json += "\"last\":" + DblStr(SymbolInfoDouble(_Symbol, SYMBOL_LAST), _Digits) + ",";
   json += "\"change\":" + DblStr(change, _Digits) + ",";
   json += "\"account\":{";
   json += "\"balance\":" + DoubleToString(balance, 2) + ",";
   json += "\"equity\":" + DoubleToString(equity, 2) + ",";
   json += "\"margin\":" + DoubleToString(margin, 2) + ",";
   json += "\"free_margin\":" + DoubleToString(margin_free, 2) + ",";
   json += "\"margin_level\":" + DoubleToString(margin_level, 1) + ",";
   json += "\"open_positions\":" + IntegerToString(positions_total) + ",";
   json += "\"buy_positions\":" + IntegerToString(buy_count) + ",";
   json += "\"sell_positions\":" + IntegerToString(sell_count) + ",";
   json += "\"buy_volume\":" + DoubleToString(buy_volume, 2) + ",";
   json += "\"sell_volume\":" + DoubleToString(sell_volume, 2) + ",";
   json += "\"floating_profit\":" + DoubleToString(total_profit, 2);
   json += "},";
   json += "\"indicators\":" + ind + ",";
   json += "\"structure_h50\":" + DblStr(highest_50, _Digits) + ",";
   json += "\"structure_l50\":" + DblStr(lowest_50, _Digits) + ",";
   json += "\"recent_candles\":" + candles;
   json += "}";

   char post_data[], result_data[];
   string result_headers;
   int len = StringToCharArray(json, post_data, 0, WHOLE_ARRAY, CP_UTF8);
   if(len > 0) ArrayResize(post_data, len - 1);

   int res = WebRequest("POST", ServerURL, "Content-Type: application/json\r\n", 1500, post_data, result_data, result_headers);
   if(res == -1 && EnableLogging)
   {
      int err = GetLastError();
      if(err == 4060)
         Print("AI Trader: Add '", ServerURL, "' to Tools > Options > Expert Advisors > WebRequest URLs");
   }

   is_sending = false;
}

//+------------------------------------------------------------------+
//| Send Extended Data                                               |
//+------------------------------------------------------------------+
void SendExtendedData()
{
   if(is_sending) return;
   is_sending = true;

   string json = "{";
   json += "\"symbol\":\"" + _Symbol + "\",";
   json += "\"timeframe\":\"" + EnumToString(_Period) + "\",";
   json += "\"extended\":true,";
   json += "\"symbol_info\":" + BuildSymbolInfo() + ",";
   json += "\"mtf\":" + BuildMTFSummary() + ",";
   json += "\"correlation\":" + BuildCorrelationData() + ",";
   json += "\"calendar\":" + BuildEconomicCalendar() + ",";

   MqlRates rates[];
   ArraySetAsSeries(rates, true);
   int copied = CopyRates(_Symbol, _Period, 0, 200, rates);
   if(copied > 0)
   {
      json += "\"structure\":" + BuildMarketStructure(rates, copied) + ",";
      json += "\"volume_profile\":" + BuildVolumeProfile(rates, copied) + ",";
      json += "\"patterns\":" + BuildPatterns(rates, copied) + ",";
      json += "\"advanced_metrics\":" + BuildAdvancedMetrics(rates, copied) + ",";
   }

   json += "\"session\":" + BuildSessionInfo() + ",";
   json += "\"pending_orders\":" + BuildPendingOrders() + ",";
   json += "\"recent_trades\":" + BuildRecentTrades();
   json += "}";

   char post_data[], result_data[];
   string result_headers;
   int len = StringToCharArray(json, post_data, 0, WHOLE_ARRAY, CP_UTF8);
   if(len > 0) ArrayResize(post_data, len - 1);

   WebRequest("POST", ServerURL, "Content-Type: application/json\r\n", 2000, post_data, result_data, result_headers);
   is_sending = false;
}

//+------------------------------------------------------------------+
//| Build Symbol Info                                                |
//+------------------------------------------------------------------+
string BuildSymbolInfo()
{
   string j = "{";
   j += "\"digits\":" + IntegerToString((int)SymbolInfoInteger(_Symbol, SYMBOL_DIGITS)) + ",";
   j += "\"point\":" + DoubleToString(SymbolInfoDouble(_Symbol, SYMBOL_POINT), _Digits) + ",";
   j += "\"contract_size\":" + DoubleToString(SymbolInfoDouble(_Symbol, SYMBOL_TRADE_CONTRACT_SIZE), 0) + ",";
   j += "\"tick_value\":" + DoubleToString(SymbolInfoDouble(_Symbol, SYMBOL_TRADE_TICK_VALUE), 5) + ",";
   j += "\"tick_size\":" + DoubleToString(SymbolInfoDouble(_Symbol, SYMBOL_TRADE_TICK_SIZE), _Digits) + ",";
   j += "\"volume_min\":" + DoubleToString(SymbolInfoDouble(_Symbol, SYMBOL_VOLUME_MIN), 2) + ",";
   j += "\"volume_max\":" + DoubleToString(SymbolInfoDouble(_Symbol, SYMBOL_VOLUME_MAX), 2) + ",";
   j += "\"volume_step\":" + DoubleToString(SymbolInfoDouble(_Symbol, SYMBOL_VOLUME_STEP), 2) + ",";
   j += "\"swap_long\":" + DoubleToString(SymbolInfoDouble(_Symbol, SYMBOL_SWAP_LONG), 2) + ",";
   j += "\"swap_short\":" + DoubleToString(SymbolInfoDouble(_Symbol, SYMBOL_SWAP_SHORT), 2) + ",";
   j += "\"margin_initial\":" + DoubleToString(SymbolInfoDouble(_Symbol, SYMBOL_MARGIN_INITIAL), 2) + ",";
   j += "\"trade_mode\":" + IntegerToString((int)SymbolInfoInteger(_Symbol, SYMBOL_TRADE_MODE)) + ",";
   j += "\"spread_float\":" + (SymbolInfoInteger(_Symbol, SYMBOL_SPREAD_FLOAT) ? "true" : "false") + ",";
   j += "\"session_high\":" + DblStr(SymbolInfoDouble(_Symbol, SYMBOL_BIDHIGH), _Digits) + ",";
   j += "\"session_low\":" + DblStr(SymbolInfoDouble(_Symbol, SYMBOL_BIDLOW), _Digits) + ",";
   j += "\"session_aw\":" + DblStr(SymbolInfoDouble(_Symbol, SYMBOL_SESSION_AW), _Digits);
   j += "}";
   return j;
}

//+------------------------------------------------------------------+
//| Build Correlation Data                                           |
//+------------------------------------------------------------------+
string BuildCorrelationData()
{
   string j = "{";
   for(int i = 0; i < corr_count; i++)
   {
      string sym = corr_symbols[i];
      if(i > 0) j += ",";
      
      double cbid = SymbolInfoDouble(sym, SYMBOL_BID);
      double cask = SymbolInfoDouble(sym, SYMBOL_ASK);
      double cchange = 0;
      int cdigits = (int)SymbolInfoInteger(sym, SYMBOL_DIGITS);
      
      MqlRates cr[];
      ArraySetAsSeries(cr, true);
      if(CopyRates(sym, _Period, 0, 2, cr) >= 2 && cr[1].close > 0)
         cchange = ((cbid - cr[1].close) / cr[1].close) * 100.0;
      
      j += "\"" + sym + "\":{";
      j += "\"bid\":" + DblStr(cbid, cdigits) + ",";
      j += "\"ask\":" + DblStr(cask, cdigits) + ",";
      double cpoint = SymbolInfoDouble(sym, SYMBOL_POINT);
      double cspread = (cpoint > 0) ? MathAbs(cask - cbid) / cpoint : 0;
      j += "\"spread\":" + DblStr(cspread, 1) + ",";
      j += "\"change_pct\":" + DblStr(cchange, 3);
      j += "}";
   }
   j += "}";
   return j;
}

//+------------------------------------------------------------------+
//| Build Economic Calendar                                          |
//+------------------------------------------------------------------+
string BuildEconomicCalendar()
{
   string j = "[";
   int count = 0;
   datetime now = TimeCurrent();
   datetime future = now + 86400 * 3;
   
   string currency = StringSubstr(_Symbol, 0, 3);
   MqlCalendarValue values[];
   if(CalendarValueHistory(values, now, future, NULL, currency))
   {
      for(int i = 0; i < ArraySize(values) && count < 10; i++)
      {
         MqlCalendarEvent ev;
         if(CalendarEventById(values[i].event_id, ev))
         {
            if(count > 0) j += ",";
            j += "{\"time\":" + IntegerToString((long)values[i].time);
            j += ",\"id\":" + IntegerToString((long)ev.id);
            j += ",\"name\":\"" + ev.name + "\"";
            j += ",\"importance\":" + IntegerToString((int)ev.importance);
            j += "}";
            count++;
         }
      }
   }
   j += "]";
   return j;
}

//+------------------------------------------------------------------+
//| Build Market Structure                                           |
//+------------------------------------------------------------------+
string BuildMarketStructure(MqlRates &rates[], int count)
{
   if(count < 20) return "{}";
   
   string j = "{";
   
   double supports[5], resistances[5];
   int sup_count = 0, res_count = 0;
   
   for(int i = 2; i < MathMin(count, 100) && (sup_count < 5 || res_count < 5); i++)
   {
      bool is_low = true, is_high = true;
      for(int k = 1; k <= 2; k++)
      {
         if(rates[i-k].low <= rates[i].low || rates[i+k].low <= rates[i].low) is_low = false;
         if(rates[i-k].high >= rates[i].high || rates[i+k].high >= rates[i].high) is_high = false;
      }
      if(is_low && sup_count < 5) { supports[sup_count++] = rates[i].low; }
      if(is_high && res_count < 5) { resistances[res_count++] = rates[i].high; }
   }
   
   j += "\"support_levels\":[";
   for(int i = 0; i < sup_count; i++) { if(i > 0) j += ","; j += DoubleToString(supports[i], _Digits); }
   j += "],\"resistance_levels\":[";
   for(int i = 0; i < res_count; i++) { if(i > 0) j += ","; j += DoubleToString(resistances[i], _Digits); }
   j += "],";
   
   double swing_high = rates[0].high, swing_low = rates[0].low;
   for(int i = 0; i < MathMin(count, 100); i++)
   {
      if(rates[i].high > swing_high) swing_high = rates[i].high;
      if(rates[i].low < swing_low) swing_low = rates[i].low;
   }
   double fib_range = swing_high - swing_low;
   j += "\"fibonacci\":{";
   j += "\"0\":" + DoubleToString(swing_low, _Digits) + ",";
   j += "\"0.236\":" + DoubleToString(swing_low + fib_range * 0.236, _Digits) + ",";
   j += "\"0.382\":" + DoubleToString(swing_low + fib_range * 0.382, _Digits) + ",";
   j += "\"0.5\":" + DoubleToString(swing_low + fib_range * 0.5, _Digits) + ",";
   j += "\"0.618\":" + DoubleToString(swing_low + fib_range * 0.618, _Digits) + ",";
   j += "\"0.786\":" + DoubleToString(swing_low + fib_range * 0.786, _Digits) + ",";
   j += "\"1\":" + DoubleToString(swing_high, _Digits);
   j += "},";
   
   double prev_high = rates[0].high, prev_low = rates[0].low, prev_close = rates[0].close;
   for(int i = 0; i < count; i++)
   {
      if(rates[i].time < rates[0].time - 86400) break;
      if(rates[i].high > prev_high) prev_high = rates[i].high;
      if(rates[i].low < prev_low) prev_low = rates[i].low;
      prev_close = rates[i].close;
   }
   double pivot = (prev_high + prev_low + prev_close) / 3.0;
   j += "\"pivots\":{";
   j += "\"pivot\":" + DoubleToString(pivot, _Digits) + ",";
   j += "\"r1\":" + DoubleToString(2.0 * pivot - prev_low, _Digits) + ",";
   j += "\"r2\":" + DoubleToString(pivot + (prev_high - prev_low), _Digits) + ",";
   j += "\"r3\":" + DoubleToString(prev_high + 2.0 * (pivot - prev_low), _Digits) + ",";
   j += "\"s1\":" + DoubleToString(2.0 * pivot - prev_high, _Digits) + ",";
   j += "\"s2\":" + DoubleToString(pivot - (prev_high - prev_low), _Digits) + ",";
   j += "\"s3\":" + DoubleToString(prev_low - 2.0 * (prev_high - pivot), _Digits);
   j += "},";
   
   int higher_highs = 0, lower_lows = 0, lower_highs = 0, higher_lows = 0;
   for(int i = 1; i < MathMin(count, 50); i++)
   {
      if(rates[i].high > rates[i-1].high) higher_highs++;
      if(rates[i].low < rates[i-1].low) lower_lows++;
      if(rates[i].high < rates[i-1].high) lower_highs++;
      if(rates[i].low > rates[i-1].low) higher_lows++;
   }
   string trend = "neutral";
   if(higher_highs > lower_highs && higher_lows > lower_lows) trend = "uptrend";
   else if(lower_highs > higher_highs && lower_lows > higher_lows) trend = "downtrend";
   j += "\"trend\":\"" + trend + "\",";
   
   double atr_vals[14];
   if(CopyBuffer(h_atr, 0, 0, 14, atr_vals) >= 14)
   {
      double avg_atr = 0;
      for(int i = 0; i < 14; i++) avg_atr += atr_vals[i];
      avg_atr /= 14.0;
      double current_atr = atr_vals[0];
      string regime = "normal";
      if(current_atr > avg_atr * 1.5) regime = "high_volatility";
      else if(current_atr < avg_atr * 0.5) regime = "low_volatility";
      j += "\"volatility_regime\":\"" + regime + "\",";
   }
   
   j += "\"swing_high\":" + DoubleToString(swing_high, _Digits) + ",";
   j += "\"swing_low\":" + DoubleToString(swing_low, _Digits);
   j += "}";
   
   return j;
}

//+------------------------------------------------------------------+
//| Build MTF Summary                                                |
//+------------------------------------------------------------------+
string BuildMTFSummary()
{
   string j = "{";
   for(int i = 0; i < 8; i++)
   {
      if(i > 0) j += ",";
      
      MqlRates cr[];
      ArraySetAsSeries(cr, true);
      int cc = CopyRates(_Symbol, mtf_periods[i], 0, 10, cr);
      
      double rsi_val = 50, sma_val = 0, atr_val = 0, macd_val = 0;
      double buf[1];
      
      if(h_mtf_rsi[i] != INVALID_HANDLE && CopyBuffer(h_mtf_rsi[i], 0, 0, 1, buf) > 0) rsi_val = buf[0];
      if(h_mtf_sma[i] != INVALID_HANDLE && CopyBuffer(h_mtf_sma[i], 0, 0, 1, buf) > 0) sma_val = buf[0];
      if(h_mtf_atr[i] != INVALID_HANDLE && CopyBuffer(h_mtf_atr[i], 0, 0, 1, buf) > 0) atr_val = buf[0];
      if(h_mtf_macd[i] != INVALID_HANDLE && CopyBuffer(h_mtf_macd[i], 0, 0, 1, buf) > 0) macd_val = buf[0];
      
      double close = (cc > 0) ? cr[0].close : 0;
      double change = (cc > 1 && cr[1].close > 0) ? ((close - cr[1].close) / cr[1].close) * 100.0 : 0;
      
      string tf_trend = "neutral";
      if(rsi_val > 60) tf_trend = "bullish";
      else if(rsi_val < 40) tf_trend = "bearish";
      
      j += "\"" + mtf_names[i] + "\":{";
      j += "\"close\":" + DblStr(close, _Digits) + ",";
      j += "\"change_pct\":" + DblStr(change, 3) + ",";
      j += "\"rsi\":" + DblStr(rsi_val, 1) + ",";
      j += "\"sma_20\":" + DblStr(sma_val, _Digits) + ",";
      j += "\"atr\":" + DblStr(atr_val, _Digits) + ",";
      j += "\"macd\":" + DblStr(macd_val, _Digits) + ",";
      j += "\"trend\":\"" + tf_trend + "\",";
      j += "\"candle_count\":" + IntegerToString(cc);
      j += "}";
   }
   j += "}";
   return j;
}

//+------------------------------------------------------------------+
//| Build Volume Profile                                             |
//+------------------------------------------------------------------+
string BuildVolumeProfile(MqlRates &rates[], int count)
{
   if(count < 10) return "{}";
   
   string j = "{";
   
   double high = rates[0].high, low = rates[0].low;
   for(int i = 0; i < count; i++)
   {
      if(rates[i].high > high) high = rates[i].high;
      if(rates[i].low < low) low = rates[i].low;
   }
   
   int levels = 20;
   double step = (high - low) / (double)levels;
   if(step <= 0) return "{}";
   
   double volumes[];
   ArrayResize(volumes, levels);
   double prices[];
   ArrayResize(prices, levels);
   ArrayInitialize(volumes, 0);
   
   for(int i = 0; i < count; i++)
   {
      int idx = (int)((rates[i].close - low) / step);
      if(idx >= 0 && idx < levels)
      {
         volumes[idx] += (double)rates[i].tick_volume;
         prices[idx] = low + step * ((double)idx + 0.5);
      }
   }
   
   double max_vol = 0;
   int poc_idx = 0;
   for(int i = 0; i < levels; i++)
   {
      if(volumes[i] > max_vol) { max_vol = volumes[i]; poc_idx = i; }
   }
   
   j += "\"poc\":" + DoubleToString(prices[poc_idx], _Digits) + ",";
   j += "\"value_area_high\":" + DoubleToString(prices[MathMin(poc_idx + 3, levels - 1)], _Digits) + ",";
   j += "\"value_area_low\":" + DoubleToString(prices[MathMax(poc_idx - 3, 0)], _Digits) + ",";
   j += "\"total_volume\":" + DoubleToString(max_vol, 0) + ",";
   j += "\"levels\":[";
   
   for(int i = 0; i < levels; i++)
   {
      if(i > 0) j += ",";
      j += "{\"price\":" + DoubleToString(prices[i], _Digits) + ",\"volume\":" + DoubleToString(volumes[i], 0) + "}";
   }
   j += "]";
   j += "}";
   
   return j;
}

//+------------------------------------------------------------------+
//| Build Session Info                                               |
//+------------------------------------------------------------------+
string BuildSessionInfo()
{
   datetime now = TimeCurrent();
   MqlDateTime dt;
   TimeToStruct(now, dt);
   int hour = dt.hour;
   
   string session = "asian";
   if(hour >= 8 && hour < 12) session = "london";
   else if(hour >= 13 && hour < 17) session = "new_york";
   else if(hour >= 22 || hour < 2) session = "sydney";
   
   string j = "{";
   j += "\"current\":\"" + session + "\",";
   j += "\"hour_utc\":" + IntegerToString(hour) + ",";
   j += "\"is_overlap\":" + ((hour >= 8 && hour < 9) || (hour >= 13 && hour < 14) ? "true" : "false");
   j += "}";
   
   return j;
}

//+------------------------------------------------------------------+
//| Build Candlestick Patterns                                       |
//+------------------------------------------------------------------+
string BuildPatterns(MqlRates &rates[], int count)
{
   if(count < 5) return "[]";
   
   string j = "[";
   int pat_count = 0;
   
   double body0 = MathAbs(rates[0].close - rates[0].open);
   double range0 = rates[0].high - rates[0].low;
   
   if(range0 > 0 && body0 < range0 * 0.1)
   {
      j += "{\"pattern\":\"DOJI\",\"candle\":0,\"reliability\":70}";
      pat_count++;
   }
   
   double lower_shadow = MathMin(rates[0].open, rates[0].close) - rates[0].low;
   double upper_shadow = rates[0].high - MathMax(rates[0].open, rates[0].close);
   
   if(range0 > 0)
   {
      if(lower_shadow > body0 * 2.0 && upper_shadow < range0 * 0.1)
      {
         if(pat_count > 0) j += ",";
         j += "{\"pattern\":\"HAMMER\",\"candle\":0,\"reliability\":75}";
         pat_count++;
      }
      if(upper_shadow > body0 * 2.0 && lower_shadow < range0 * 0.1)
      {
         if(pat_count > 0) j += ",";
         j += "{\"pattern\":\"INVERTED_HAMMER\",\"candle\":0,\"reliability\":70}";
         pat_count++;
      }
   }
   
   if(count >= 2)
   {
      double prev_body = MathAbs(rates[1].close - rates[1].open);
      bool prev_bullish = rates[1].close > rates[1].open;
      bool curr_bullish = rates[0].close > rates[0].open;
      
      if(!prev_bullish && curr_bullish && rates[0].open <= rates[1].close && rates[0].close >= rates[1].open)
      {
         if(pat_count > 0) j += ",";
         j += "{\"pattern\":\"BULLISH_ENGULFING\",\"candle\":0,\"reliability\":80}";
         pat_count++;
      }
      if(prev_bullish && !curr_bullish && rates[0].open >= rates[1].close && rates[0].close <= rates[1].open)
      {
         if(pat_count > 0) j += ",";
         j += "{\"pattern\":\"BEARISH_ENGULFING\",\"candle\":0,\"reliability\":80}";
         pat_count++;
      }
   }
   
   if(count >= 3)
   {
      bool m1_bull = rates[2].close > rates[2].open;
      bool m2_bull = rates[1].close > rates[1].open;
      bool m3_bull = rates[0].close > rates[0].open;
      
      if(!m1_bull && !m2_bull && m3_bull && rates[0].close > (rates[2].open + rates[2].close) / 2.0)
      {
         if(pat_count > 0) j += ",";
         j += "{\"pattern\":\"MORNING_STAR\",\"candle\":0,\"reliability\":85}";
         pat_count++;
      }
      if(m1_bull && m2_bull && !m3_bull && rates[0].close < (rates[2].open + rates[2].close) / 2.0)
      {
         if(pat_count > 0) j += ",";
         j += "{\"pattern\":\"EVENING_STAR\",\"candle\":0,\"reliability\":85}";
         pat_count++;
      }
   }
   
   j += "]";
   return j;
}

//+------------------------------------------------------------------+
//| Build Advanced Metrics                                           |
//+------------------------------------------------------------------+
string BuildAdvancedMetrics(MqlRates &rates[], int count)
{
   if(count < 20) return "{}";
   
   string j = "{";
   
   double returns[19];
   for(int i = 0; i < 19; i++)
   {
      if(rates[i+1].close > 0)
         returns[i] = MathLog(rates[i].close / rates[i+1].close);
      else
         returns[i] = 0;
   }
   
   double mean = 0;
   for(int i = 0; i < 19; i++) mean += returns[i];
   mean /= 19.0;
   
   double variance = 0;
   for(int i = 0; i < 19; i++) variance += MathPow(returns[i] - mean, 2);
   variance /= 18.0;
   
   double daily_vol = MathSqrt(variance);
   double annual_vol = daily_vol * MathSqrt(252.0) * 100.0;
   
   j += "\"annualized_volatility\":" + DblStr(annual_vol, 2) + ",";
   j += "\"daily_volatility\":" + DblStr(daily_vol * 100.0, 2) + ",";
   
   double atr_vals[20];
   if(CopyBuffer(h_atr, 0, 0, 20, atr_vals) >= 20)
   {
      double avg_atr = 0;
      for(int i = 1; i < 20; i++) avg_atr += atr_vals[i];
      avg_atr /= 19.0;
      if(avg_atr > 0)
         j += "\"atr_ratio\":" + DblStr(atr_vals[0] / avg_atr, 2) + ",";
      else
         j += "\"atr_ratio\":1.0,";
   }
   
   double rsi_vals[20];
   if(CopyBuffer(h_rsi, 0, 0, 20, rsi_vals) >= 20)
   {
      bool price_higher = rates[0].close > rates[5].close;
      bool rsi_lower = rsi_vals[0] < rsi_vals[5];
      bool bearish_div = price_higher && rsi_lower;
      
      bool price_lower = rates[0].close < rates[5].close;
      bool rsi_higher = rsi_vals[0] > rsi_vals[5];
      bool bullish_div = price_lower && rsi_higher;
      
      string div = "none";
      if(bullish_div) div = "bullish";
      else if(bearish_div) div = "bearish";
      j += "\"rsi_divergence\":\"" + div + "\",";
   }
   
   double vol_avg_5 = 0, vol_avg_20 = 0;
   int c5 = MathMin(5, count), c20 = MathMin(20, count);
   for(int i = 0; i < c5; i++) vol_avg_5 += (double)rates[i].tick_volume;
   for(int i = 0; i < c20; i++) vol_avg_20 += (double)rates[i].tick_volume;
   vol_avg_5 /= (double)c5;
   vol_avg_20 /= (double)c20;
   
   j += "\"volume_ratio\":" + DblStr(vol_avg_5 / MathMax(vol_avg_20, 1.0), 2) + ",";
   
   string vol_trend = "stable";
   if(vol_avg_5 > vol_avg_20 * 1.2) vol_trend = "increasing";
   else if(vol_avg_5 < vol_avg_20 * 0.8) vol_trend = "decreasing";
   j += "\"volume_trend\":\"" + vol_trend + "\"";
   
   j += "}";
   return j;
}

//+------------------------------------------------------------------+
//| Build Pending Orders                                             |
//+------------------------------------------------------------------+
string BuildPendingOrders()
{
   string j = "[";
   int order_count = 0;
   int orders_total = OrdersTotal();
   
   for(int i = orders_total - 1; i >= 0; i--)
   {
      ulong ticket = OrderGetTicket(i);
      if(ticket > 0)
      {
         if(OrderGetString(ORDER_SYMBOL) == _Symbol && (int)OrderGetInteger(ORDER_MAGIC) == MagicNumber)
         {
            if(order_count > 0) j += ",";
            j += "{\"ticket\":" + IntegerToString((long)ticket);
            
            ENUM_ORDER_TYPE otype = (ENUM_ORDER_TYPE)OrderGetInteger(ORDER_TYPE);
            string typeStr = "UNKNOWN";
            if(otype == ORDER_TYPE_BUY_LIMIT) typeStr = "BUY_LIMIT";
            else if(otype == ORDER_TYPE_SELL_LIMIT) typeStr = "SELL_LIMIT";
            else if(otype == ORDER_TYPE_BUY_STOP) typeStr = "BUY_STOP";
            else if(otype == ORDER_TYPE_SELL_STOP) typeStr = "SELL_STOP";
            
            j += ",\"type\":\"" + typeStr + "\"";
            j += ",\"price\":" + DoubleToString(OrderGetDouble(ORDER_PRICE_OPEN), _Digits);
            j += ",\"sl\":" + DoubleToString(OrderGetDouble(ORDER_SL), _Digits);
            j += ",\"tp\":" + DoubleToString(OrderGetDouble(ORDER_TP), _Digits);
            j += ",\"volume\":" + DoubleToString(OrderGetDouble(ORDER_VOLUME_INITIAL), 2);
            j += "}";
            order_count++;
         }
      }
   }
   j += "]";
   return j;
}

//+------------------------------------------------------------------+
//| Build Recent Trades                                              |
//+------------------------------------------------------------------+
string BuildRecentTrades()
{
   string j = "[";
   int trade_count = 0;
   datetime now = TimeCurrent();
   
   if(HistorySelect(now - 86400 * 7, now))
   {
      int deals = HistoryDealsTotal();
      for(int i = deals - 1; i >= 0 && trade_count < 10; i--)
      {
         ulong ticket = HistoryDealGetTicket(i);
         if(ticket > 0)
         {
            if(HistoryDealGetString(ticket, DEAL_SYMBOL) == _Symbol && HistoryDealGetInteger(ticket, DEAL_MAGIC) == MagicNumber)
            {
               if(trade_count > 0) j += ",";
               j += "{\"time\":" + IntegerToString((long)HistoryDealGetInteger(ticket, DEAL_TIME));
               
               ENUM_DEAL_TYPE dtype = (ENUM_DEAL_TYPE)HistoryDealGetInteger(ticket, DEAL_TYPE);
               string typeStr = (dtype == DEAL_TYPE_BUY) ? "BUY" : "SELL";
               
               j += ",\"type\":\"" + typeStr + "\"";
               j += ",\"volume\":" + DoubleToString(HistoryDealGetDouble(ticket, DEAL_VOLUME), 2);
               j += ",\"price\":" + DoubleToString(HistoryDealGetDouble(ticket, DEAL_PRICE), _Digits);
               j += ",\"profit\":" + DoubleToString(HistoryDealGetDouble(ticket, DEAL_PROFIT), 2);
               j += "}";
               trade_count++;
            }
         }
      }
   }
   j += "]";
   return j;
}

//+------------------------------------------------------------------+
//| Check for AI Trading Signals                                     |
//+------------------------------------------------------------------+
void CheckForSignals()
{
   if(!EnableAutoTrading) return;

   char data[], result[];
   string headers;
   string url = SignalCheckURL + "?symbol=" + _Symbol;
   int res = WebRequest("GET", url, "Content-Type: application/json\r\n", 3000, data, result, headers);
   if(res != 200) return;

   string response = CharArrayToString(result);
   if(StringLen(response) < 10 || StringFind(response, "[]") >= 0) return;

   int startPos = 0;
   while(true)
   {
      int signalPos = StringFind(response, "{", startPos);
      if(signalPos < 0) break;
      
      int endPos = StringFind(response, "}", signalPos);
      if(endPos < 0) break;
      
      string signalObj = StringSubstr(response, signalPos, endPos - signalPos + 1);
      startPos = endPos + 1;

      string sym = ExtractJSONValue(signalObj, "symbol");
      string dir = ExtractJSONValue(signalObj, "direction");
      double entry = StringToDouble(ExtractJSONValue(signalObj, "entry_price"));
      double sl = StringToDouble(ExtractJSONValue(signalObj, "stop_loss"));
      double tp = StringToDouble(ExtractJSONValue(signalObj, "take_profit"));

      if(sym == _Symbol && entry > 0 && sl > 0)
      {
         ExecuteSignal(dir, entry, sl, tp);
      }
   }
}

//+------------------------------------------------------------------+
//| Execute Trading Signal                                           |
//+------------------------------------------------------------------+
void ExecuteSignal(string direction, double entry, double sl, double tp)
{
   if(IsPositionOpen(_Symbol)) return;

   double lotSize = DefaultLotSize;
   double riskAmount = AccountInfoDouble(ACCOUNT_BALANCE) * (RiskPerTrade / 100.0);
   double riskPips = MathAbs(entry - sl) / _Point;
   if(riskPips > 0)
   {
      double tickValue = SymbolInfoDouble(_Symbol, SYMBOL_TRADE_TICK_VALUE);
      if(tickValue > 0)
         lotSize = riskAmount / (riskPips * tickValue);
      lotSize = MathMax(MathMin(lotSize, 100.0), 0.001);
   }

   double lotStep = SymbolInfoDouble(_Symbol, SYMBOL_VOLUME_STEP);
   if(lotStep > 0) lotSize = MathRound(lotSize / lotStep) * lotStep;

   double minLot = SymbolInfoDouble(_Symbol, SYMBOL_VOLUME_MIN);
   double maxLot = SymbolInfoDouble(_Symbol, SYMBOL_VOLUME_MAX);
   lotSize = MathMax(MathMin(lotSize, maxLot), minLot);

   MqlTradeRequest request = {};
   MqlTradeResult result = {};

   uint filling = (uint)SymbolInfoInteger(_Symbol, SYMBOL_FILLING_MODE);
   if((filling & SYMBOL_FILLING_FOK) != 0) request.type_filling = ORDER_FILLING_FOK;
   else if((filling & SYMBOL_FILLING_IOC) != 0) request.type_filling = ORDER_FILLING_IOC;
   else request.type_filling = ORDER_FILLING_RETURN;

   request.action = TRADE_ACTION_DEAL;
   request.symbol = _Symbol;
   request.volume = lotSize;
   request.deviation = 10;
   request.magic = MagicNumber;

   if(direction == "LONG" || direction == "BUY")
   {
      request.type = ORDER_TYPE_BUY;
      request.price = NormalizeDouble(SymbolInfoDouble(_Symbol, SYMBOL_ASK), _Digits);
      request.sl = NormalizeDouble(sl, _Digits);
      request.tp = NormalizeDouble(tp, _Digits);
   }
   else
   {
      request.type = ORDER_TYPE_SELL;
      request.price = NormalizeDouble(SymbolInfoDouble(_Symbol, SYMBOL_BID), _Digits);
      request.sl = NormalizeDouble(sl, _Digits);
      request.tp = NormalizeDouble(tp, _Digits);
   }

   request.comment = "AI Quant v5";

   if(OrderSend(request, result))
   {
      Print("AI Signal Executed: ", direction, " ", _Symbol,
            " Lot:", DoubleToString(lotSize, 2),
            " SL:", DoubleToString(sl, _Digits),
            " TP:", DoubleToString(tp, _Digits));
   }
   else
   {
      Print("AI Signal FAILED: ", direction, " ", _Symbol,
            " Error:", IntegerToString(result.retcode));
   }

   string logEntry = "{";
   logEntry += "\"event_type\":\"trade_executed\",";
   logEntry += "\"message\":\"" + direction + " " + _Symbol + "\",";
   logEntry += "\"details\":{\"symbol\":\"" + _Symbol + "\",\"direction\":\"" + direction + "\",\"lot\":" + DoubleToString(lotSize, 2) + "}";
   logEntry += "}";

   char post_data[], result_data[];
   string result_headers;
   int len = StringToCharArray(logEntry, post_data, 0, WHOLE_ARRAY, CP_UTF8);
   if(len > 0)
   {
      ArrayResize(post_data, len - 1);
      WebRequest("POST", LogEventURL, "Content-Type: application/json\r\n", 1000, post_data, result_data, result_headers);
   }
}

//+------------------------------------------------------------------+
//| Helper: Check if position exists                                 |
//+------------------------------------------------------------------+
bool IsPositionOpen(string symbol)
{
   for(int i = PositionsTotal() - 1; i >= 0; i--)
   {
      ulong ticket = PositionGetTicket(i);
      if(ticket > 0 && PositionSelectByTicket(ticket))
      {
         if(PositionGetString(POSITION_SYMBOL) == symbol &&
            (int)PositionGetInteger(POSITION_MAGIC) == MagicNumber)
            return true;
      }
   }
   return false;
}

//+------------------------------------------------------------------+
//| Helper: Extract JSON string value                                |
//+------------------------------------------------------------------+
string ExtractJSONValue(string json, string key)
{
   string search = "\"" + key + "\":\"";
   int pos = StringFind(json, search);
   if(pos >= 0)
   {
      int start = pos + StringLen(search);
      int end = StringFind(json, "\"", start);
      if(end > start) return StringSubstr(json, start, end - start);
   }
   search = "\"" + key + "\":";
   pos = StringFind(json, search);
   if(pos >= 0)
   {
      int start = pos + StringLen(search);
      string remainder = StringSubstr(json, start);
      StringTrimLeft(remainder);
      string res = "";
      for(int i = 0; i < StringLen(remainder); i++)
      {
         ushort c = StringGetCharacter(remainder, i);
         if(c == ',' || c == '}' || c == ']' || (int)c == 0) break;
         res += ShortToString(c);
      }
      return res;
   }
   return "";
}

//+------------------------------------------------------------------+
//| Check for AI Drawing Commands                                    |
//+------------------------------------------------------------------+
void CheckForDrawings()
{
   char data[], result[];
   string headers;
   string url = DrawingCheckURL + "?symbol=" + _Symbol;

   int res = WebRequest("GET", url, "Content-Type: application/json\r\n", 2000, data, result, headers);
   if(res != 200) return;

   string response = CharArrayToString(result);
   if(StringFind(response, "\"commands\":[]") >= 0) return;

   ObjectsDeleteAll(0, "AI_");

   int arrStart = StringFind(response, "\"commands\":[");
   if(arrStart < 0) return;
   arrStart += 12;
   int arrEnd = StringFind(response, "]", arrStart);
   if(arrEnd < 0) return;

   string cmdBody = StringSubstr(response, arrStart, arrEnd - arrStart);

   int cmdIndex = 0;
   for(;;)
   {
      int startBrace = StringFind(cmdBody, "{");
      int endBrace = StringFind(cmdBody, "}");
      if(startBrace < 0 || endBrace < 0 || endBrace <= startBrace) break;

      string cmd = StringSubstr(cmdBody, startBrace + 1, endBrace - startBrace - 1);
      cmdBody = StringSubstr(cmdBody, endBrace + 1);
      if(StringLen(cmd) < 5) continue;

      string objType = ExtractJSONValue(cmd, "type");
      string label = ExtractJSONValue(cmd, "label");
      string colorStr = ExtractJSONValue(cmd, "color");
      color drawColor = clrAqua;
      if(colorStr == "clrRed") drawColor = clrRed;
      if(colorStr == "clrDodgerBlue") drawColor = clrDodgerBlue;

      string objName = "AI_" + objType + "_" + IntegerToString(cmdIndex) + "_" + IntegerToString(GetTickCount());

      if(objType == "HLINE")
      {
         double price = StringToDouble(ExtractJSONValue(cmd, "price"));
         if(price > 0)
         {
            ObjectCreate(0, objName, OBJ_HLINE, 0, 0, price);
            ObjectSetInteger(0, objName, OBJPROP_COLOR, drawColor);
            ObjectSetString(0, objName, OBJPROP_TEXT, label);
            ObjectSetInteger(0, objName, OBJPROP_WIDTH, 2);
            ObjectSetInteger(0, objName, OBJPROP_STYLE, STYLE_DOT);
         }
      }
      else if(objType == "RECT")
      {
         double p1 = StringToDouble(ExtractJSONValue(cmd, "price1"));
         double p2 = StringToDouble(ExtractJSONValue(cmd, "price2"));
         if(p1 > 0 && p2 > 0)
         {
            double minPrice = MathMin(p1, p2);
            double maxPrice = MathMax(p1, p2);
            ObjectCreate(0, objName, OBJ_RECTANGLE, 0,
                         TimeCurrent() - PeriodSeconds(_Period) * 30, minPrice,
                         TimeCurrent(), maxPrice);
            ObjectSetInteger(0, objName, OBJPROP_COLOR, drawColor);
            ObjectSetInteger(0, objName, OBJPROP_FILL, true);
            ObjectSetInteger(0, objName, OBJPROP_BACK, true);
            ObjectSetInteger(0, objName, OBJPROP_WIDTH, 1);
            ObjectSetString(0, objName, OBJPROP_TEXT, label);
         }
      }
      cmdIndex++;
   }
}
//+------------------------------------------------------------------+
