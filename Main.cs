public class Program
{
    public static async Task Main()
    {
        Parser parser = Parser.GetParser();
        DbRelation relation = new(parser);
    }
}
