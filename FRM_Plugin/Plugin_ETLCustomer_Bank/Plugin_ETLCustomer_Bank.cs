using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk.Messages;
using System.IO;
using System.Runtime.Serialization;
using System.Messaging;

namespace Plugin_BienBanMiaChay
{
    public class Plugin_ETLCustomer_Bank : IPlugin
    {
        IOrganizationService service = null;
        IOrganizationServiceFactory factory = null;
        void IPlugin.Execute(IServiceProvider serviceProvider)
        {
            IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
            service = factory.CreateOrganizationService(context.UserId);

            Entity target = (Entity)context.InputParameters["Target"];
            Entity tmp = service.Retrieve(target.LogicalName, target.Id, new ColumnSet(true));

            if (target.LogicalName.ToLower() == "contact" || target.LogicalName.ToLower() == "account")
            {
                if (target.Contains("statuscode") && ((OptionSetValue)target["statuscode"]).Value == 100000000)
                {
                    MessageQueue mq;

                    if (MessageQueue.Exists(@".\Private$\DynamicCRM2Oracle_" + context.OrganizationName))
                        mq = new MessageQueue(@".\Private$\DynamicCRM2Oracle_" + context.OrganizationName);
                    else
                        mq = MessageQueue.Create(@".\Private$\DynamicCRM2Oracle_" + context.OrganizationName);

                    Message m = new Message();
                    m.Body = Serialize(tmp);
                    m.Label = "cust";
                    mq.Send(m);
                }
            }
            else if (target.LogicalName.ToLower() == "new_taikhoannganhang")
            {
                Entity KH;
                if (tmp.Contains("new_khachhang"))
                {
                    KH = service.Retrieve("contact", ((EntityReference)tmp["new_khachhang"]).Id, new ColumnSet("new_socmnd", "new_makhachhang"));
                    tmp["cmnd"] = KH.Contains("new_socmnd") ? KH["new_socmnd"].ToString() : "";
                    tmp["new_makhachhang"] = KH.Contains("new_makhachhang") ? KH["new_makhachhang"].ToString() : "";
                }
                else
                {
                    KH = service.Retrieve("account", ((EntityReference)tmp["new_khachhangdoanhnghiep"]).Id, new ColumnSet("new_masothue", "new_makhachhang"));
                    tmp["cmnd"] = KH.Contains("new_masothue") ? KH["new_masothue"].ToString() : "";
                    tmp["new_makhachhang"] = KH.Contains("new_makhachhang") ? KH["new_makhachhang"].ToString() : "";
                }

                MessageQueue mq;
                if (MessageQueue.Exists(@".\Private$\DynamicCRM2Oracle_" + context.OrganizationName))
                    mq = new MessageQueue(@".\Private$\DynamicCRM2Oracle_" + context.OrganizationName);
                else
                    mq = MessageQueue.Create(@".\Private$\DynamicCRM2Oracle_" + context.OrganizationName);

                Message m = new Message();
                m.Body = Serialize(tmp);
                m.Label = "bank";
                mq.Send(m);
            }
        }

        public static string Serialize(object obj)
        {
            using (MemoryStream memoryStream = new MemoryStream())
            using (StreamReader reader = new StreamReader(memoryStream))
            {
                DataContractSerializer serializer = new DataContractSerializer(obj.GetType());
                serializer.WriteObject(memoryStream, obj);
                memoryStream.Position = 0;
                return reader.ReadToEnd();
            }
        }

        public static object Deserialize(string xml, Type toType)
        {
            using (Stream stream = new MemoryStream())
            {
                byte[] data = System.Text.Encoding.UTF8.GetBytes(xml);
                stream.Write(data, 0, data.Length);
                stream.Position = 0;
                DataContractSerializer deserializer = new DataContractSerializer(toType);
                return deserializer.ReadObject(stream);
            }
        }

    }
}
