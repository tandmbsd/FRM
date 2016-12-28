using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Messages;
using Microsoft.Xrm.Sdk.Query;

namespace Autonumber_HDTheChap
{
    public class Autonumber_HDTheChap : IPlugin
    {
        IOrganizationService service = null;
        IOrganizationServiceFactory factory = null;
        void IPlugin.Execute(IServiceProvider serviceProvider)
        {
            IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
            service = factory.CreateOrganizationService(context.UserId);
            Entity target = (Entity)context.InputParameters["Target"];

            if (target.Contains("statuscode") && ((OptionSetValue)target["statuscode"]).Value == 100000000)
            {
                Entity hdthechap = service.Retrieve(target.LogicalName, target.Id, new ColumnSet(new string[] { "new_ngaykihopdong" }));
                if (!hdthechap.Contains("new_ngaykihopdong"))
                {
                    throw new Exception("Chưa chọn ngày kí hợp đồng trong hợp đồng thế chấp");
                }
                DateTime dt = (DateTime)hdthechap["new_ngaykihopdong"];
                int year = dt.Year;

                //Entity vudautuhientai = null;
                //vudautuhientai = Vuhientai();

                string prefix = "";
                string sufix = String.Format("/TC/{0}/HĐ-TTCS", year);
                ulong currentPos = hdthechap.Attributes.Contains("bsd_currentposition") ? ulong.Parse(hdthechap["bsd_currentposition"].ToString()) : 0;
                string field = hdthechap.Attributes.Contains("bsd_fieldlogical") ? hdthechap["bsd_fieldlogical"].ToString() : string.Empty;

                if (!string.IsNullOrWhiteSpace(field))
                {
                    currentPos++;
                    var crLength = length - currentPos.ToString().Length;
                    if (crLength < 0)
                    {
                        currentPos = 1;
                        crLength = length - 1;
                    }
                    string middle = "";
                    for (int i = 0; i < crLength; i++)
                        middle += "0";

                    eField[field] = string.Format("{0}{1}{2}{3}", prefix, middle, currentPos, sufix);
                    eAu["bsd_currentposition"] = currentPos.ToString();
                    flag = true;
                    service.Update(eAu);
                }
            }
        }
    }
}
