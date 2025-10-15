using Bybit.Api.Market;
using TALib;

public class ArmedBybitMarketKline : BybitMarketKline
{
    public decimal RSI { get; set; } = 0;
    public (decimal Fast, decimal Slow, decimal Signal) MACD { get; set; } = (0, 0, 0);
    public decimal SMA = 0;
    public (decimal UpperBand, decimal MiddleBand, decimal LowerBand) BOLL = (0, 0, 0);
    public CandleType Type;
    public decimal ChangePercent;
    public decimal DownTail;          //in percents
    public decimal UpperTail;         //in percents
    public ArmedBybitMarketKline(BybitMarketKline kline)
    {
        OpenTime = kline.OpenTime;
        OpenPrice = kline.OpenPrice;
        ClosePrice = kline.ClosePrice;
        HighPrice = kline.HighPrice;
        LowPrice = kline.LowPrice;
        Volume = kline.Volume;
        QuoteVolume = kline.QuoteVolume;
        ChangePercent = (OpenPrice - ClosePrice) / (OpenPrice / 100);
        Type = ChangePercent < 0 ? CandleType.Bull : ChangePercent > 0 ? CandleType.Bear : CandleType.Zero;
        // ChangePercent = Math.Abs(ChangePercent);
        if (Type == CandleType.Bull) (UpperTail, DownTail) =
        (Math.Abs((ClosePrice - HighPrice) / (ClosePrice / 100)), (OpenPrice - LowPrice) / (OpenPrice / 100));
        else if (Type == CandleType.Bear) (UpperTail, DownTail) =
        (Math.Abs((OpenPrice - HighPrice) / (OpenPrice / 100)), (ClosePrice - LowPrice) / (ClosePrice / 100));
        else (UpperTail, DownTail) = (0, 0);
    }
    public ArmedBybitMarketKline()
    { }
    public override string ToString()
    {
        return string.Format("{8}:\n Open: {0}, Close: {1}, Max: {2}, Low: {3}, Volume: {4}, Change percent: {5} %,\n RSI: {6}, MACD: {7}, SMA: {9}, BOLL: {10} \n",
        OpenPrice, ClosePrice, HighPrice, LowPrice, Math.Round(Volume / 1000000, 2) + " M", Math.Round(ChangePercent, 2), RSI,
         $"{MACD.Signal}, {MACD.Fast}, {MACD.Slow}", OpenTime, Math.Round(SMA, 3), $"Middle band: {BOLL.MiddleBand}, Upper band {BOLL.UpperBand}, Lower band: {BOLL.LowerBand}");

    }
}

public static class IndicatorsObserver
{
    public static List<ArmedBybitMarketKline> GetArmedKlines(this List<BybitMarketKline> klines)
    {
        List<ArmedBybitMarketKline> output = new(klines.Count);
        foreach (var kline in klines) output.Add(new(kline));
        return output.GetRSI().GetMACD().GetSMA().GetBOLL();
    }

    public static List<ArmedBybitMarketKline> GetRSI(this List<ArmedBybitMarketKline> klines, int period = 14)
    {
        ReadOnlySpan<double> closePrices = klines.Select(candle => (double)candle.ClosePrice).ToArray().AsSpan();
        Span<double> result = new double[closePrices.Length];
        Range range;
        Functions.Rsi(closePrices, new Range(0, closePrices.Length - 1), result, out range, period);
        for (int i = range.Start.Value; i < range.End.Value; i++)
        {
            klines[i].RSI = (decimal)result[i - range.Start.Value];
        }
        return klines;
    }

    public static List<ArmedBybitMarketKline> GetMACD(this List<ArmedBybitMarketKline> klines, int fastPeriod = 12, int slowPeriod = 26, int signalPeriod = 9)
    {
        ReadOnlySpan<float> closePrices = klines.Select(candle => (float)candle.OpenPrice).ToArray().AsSpan();
        Span<float> fast = new float[klines.Count];
        Span<float> signal = new float[klines.Count];
        Span<float> hist = new float[klines.Count];
        Range range;
        Functions.Macd(closePrices, new Range(0, closePrices.Length - 1), fast, signal, hist, out range, fastPeriod, slowPeriod, signalPeriod);
        for (int i = range.Start.Value; i < range.End.Value; i++)
        {
            int current = i - range.Start.Value;
            klines[i - 1].MACD = ((decimal)fast[current], (decimal)signal[current], (decimal)hist[current]);
        }
        return klines;
    }

    public static List<ArmedBybitMarketKline> GetSMA(this List<ArmedBybitMarketKline> klines, int period = 50)
    {
        ReadOnlySpan<float> closePrices = klines.Select(candle => (float)candle.ClosePrice).ToArray().AsSpan();
        Span<float> sma = new float[closePrices.Length];
        Range outRange;
        Functions.Sma(closePrices, new Range(0, closePrices.Length - 1), sma, out outRange, period);
        for (int i = outRange.Start.Value; i < outRange.End.Value; i++)
        {
            klines[i].SMA = (decimal)sma[i - outRange.Start.Value];
        }
        return klines;
    }

    public static List<ArmedBybitMarketKline> GetBOLL(this List<ArmedBybitMarketKline> klines, int upper = 2, int middle = 5, int lower = 2)
    {
        ReadOnlySpan<float> closePrices = klines.Select(candle => (float)candle.ClosePrice).ToArray().AsSpan();
        Span<float> upperBand = new float[closePrices.Length];
        Span<float> middleBand = new float[closePrices.Length];
        Span<float> lowerBand = new float[closePrices.Length];
        Range range;
        Functions.Bbands(closePrices, new Range(0, closePrices.Length - 1), upperBand, middleBand, lowerBand, out range);
        for (int i = range.Start.Value; i < range.End.Value; i++)
        {
            int current = i - range.Start.Value;
            klines[i].BOLL = ((decimal)upperBand[current], (decimal)middleBand[current], (decimal)lowerBand[current]);
        }
        return klines;
    }
    
}

public enum CandleType
{
    Bull,
    Bear,
    Zero
}
