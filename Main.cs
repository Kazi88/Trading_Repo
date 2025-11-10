using static Bybit.Api.Enums.BybitInterval;
public class Program
{
    public static async Task Main()
    {
        Parser parser = Parser.GetParser();
        DbRelation relation = new(parser);
        await Signaler.CombineSignals(relation, "BTCUSDT", 200, OneMinute, FiveMinutes, OneHour);
    }
}
