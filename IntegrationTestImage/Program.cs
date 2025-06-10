using System.Threading.Tasks;

namespace IntegrationTestImage;

public class Program
{
    public static async Task Main(string[] args)
    {
        await FlinkDotNetAspire.AppHost.Program.Main(args);
    }
}
