using System;
using System.Runtime.InteropServices;
using System.Threading;
namespace Service_Syndata
{
    class Program
    {
        public static Thread oThread;
        public static FRMClient frm = new FRMClient();
        public static void Main(string[] args)
        {
            Console.WriteLine("Run");
            oThread = new Thread(new ThreadStart(frm.Syndata));
            oThread.Start();
            //oThread.IsBackground = true;
        }
        static bool ConsoleEventCallback(int eventType)
        {
            if (eventType == 2)
            {
                Console.WriteLine("Da dong");
                oThread.Abort();
            }
            return false;
        }
        static ConsoleEventDelegate handler;
        private delegate bool ConsoleEventDelegate(int eventType);
        [DllImport("kernel32.dll", SetLastError = true)]
        private static extern bool SetConsoleCtrlHandler(ConsoleEventDelegate callback, bool add);
    }
}
