using System;
using System.Linq;
using System.IO.Ports;
using System.IO;

namespace TestCom
{
    class Program
    {
        static void Main(string[] args)
        {
            //SerialPort mySerialPort = new SerialPort("COM1");

            //mySerialPort.BaudRate = 9600;
            //mySerialPort.Parity = Parity.None;
            //mySerialPort.StopBits = StopBits.One;
            //mySerialPort.DataBits = 8;
            //mySerialPort.Handshake = Handshake.None;

            //mySerialPort.DataReceived += new SerialDataReceivedEventHandler(DataReceivedHandler);

            //mySerialPort.Open();

            //Console.WriteLine("Press any key to continue...");
            //Console.WriteLine();
            //Console.ReadKey();
            //mySerialPort.Close();

            DateTime christmas = new DateTime(2008, 12, 25);
            DateTime newYears = new DateTime(2009, 1, 1);
            int a = (int)christmas.Date.Subtract(newYears.Date).TotalDays;
            //Console.WriteLine(span);
            Console.WriteLine(a);
        }

        private static void DataReceivedHandler(object sender, SerialDataReceivedEventArgs e)
        {
            //System.IO.StreamWriter str = System.IO.File.AppendText(@"1.txt");
            //str.WriteLine(indata);
            //str.Close();
            int count = 0;
            string data = "";
            double prevalue = 0;

            try
            {
                SerialPort sp = (SerialPort)sender;
                string indata = sp.ReadExisting();
                data += indata;
                if (indata.Trim() == "")
                {
                    string chuan = "";
                    foreach (char a in data)
                        if ((int)a >= 48 && (int)a <= 57)
                            chuan += a;
                    int kq = 0;
                    int.TryParse(chuan, out kq);
                    if (kq != 0)
                    {
                        if (prevalue == kq || prevalue == 0)
                        {
                            count++;
                            prevalue = kq;
                        }
                        else
                        {
                            count = 0;
                            prevalue = kq;
                        }

                        if (count == 10)
                        {
                            Console.WriteLine(kq);
                            count = 0;
                            prevalue = 0;
                        }
                    }
                    data = "";
                }

            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
            }
        }

        public static double tachchuoiTapchat(string solieu)
        {
            Console.WriteLine("Bat dau tach chuoi ...");
            string SEPERATOR = " ";
            double t = 0;
            string[] parts = solieu.Split(new string[] { SEPERATOR, "=" }, StringSplitOptions.RemoveEmptyEntries);
            if (parts.Count() == 4)
            {
                if (Double.TryParse(parts[2], out t))
                {
                    t = double.Parse(parts[2]) / 1000;
                }
            }

            return t;
        }
    }
}
