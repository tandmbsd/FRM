using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DistincString
{
    class Program
    {
        static void Main(string[] args)
        {
            List<string> a = new List<string>();
            a.AddRange(new string[] { "a", "b", "a", "c", "d", "b" });


            Dictionary<string, bool> Distinct = new Dictionary<string, bool>();
            foreach (string value in a)
            {
                Distinct[value] = true;
            }


            List<string> b = new List<string>();
            b.AddRange(Distinct.Keys);

            foreach (string temp in b)
            {
                Console.WriteLine(temp);
            }
        }
    }
}
