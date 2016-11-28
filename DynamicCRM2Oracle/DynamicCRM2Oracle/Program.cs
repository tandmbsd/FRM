using System;
using System.Collections.Generic;
using System.Linq;
using System.ServiceProcess;
using System.Text;
using System.Threading.Tasks;

namespace DynamicCRM2Oracle
{
    class Program
    {
        static void Main(string[] args)
        {
            var service = new DynamicCRM2Oracle();
            service.Run(args);

            //ServiceBase[] ServicesToRun;
            //ServicesToRun = new ServiceBase[]
            //{
            //    new DynamicCRM2Oracle()
            //};
            //ServiceBase.Run(ServicesToRun);
        }
    }
}
