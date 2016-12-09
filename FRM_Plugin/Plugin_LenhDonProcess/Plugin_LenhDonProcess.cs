using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk.Messages;

namespace Plugin_LenhDonProcess
{
    public class Plugin_LenhDonProcess : IPlugin
    {
        IOrganizationService service = null;
        IOrganizationServiceFactory factory = null;
        void IPlugin.Execute(IServiceProvider serviceProvider)
        {
            IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
            service = factory.CreateOrganizationService(context.UserId);
            
            Entity target = (Entity)context.InputParameters["Target"];
            Entity PostLenhDon = (Entity)context.PostEntityImages["PostImage"];

            Entity myLenhDon = new Entity("new_lenhdon");
            myLenhDon.Id = target.Id;

            if (target.Contains("new_phieucanvao"))
            {
                Entity phieucanvao = service.Retrieve("new_phieucan", ((EntityReference)PostLenhDon["new_phieucanvao"]).Id, new ColumnSet(true));
                myLenhDon["new_trongluongxoi"] = phieucanvao["new_trongluong"];
            }
            if (target.Contains("new_phieucanra"))
            {
                Entity phieucanra = service.Retrieve("new_phieucan", ((EntityReference)PostLenhDon["new_phieucanra"]).Id, new ColumnSet(true));
                myLenhDon["new_trongluongbi"] = phieucanra["new_trongluong"];
            }
            if (target.Contains("new_phieudotapchat"))
            {
                Entity phieudotapchat =  service.Retrieve("new_phieudotapchat", ((EntityReference)PostLenhDon["new_phieudotapchat"]).Id, new ColumnSet(true));
                myLenhDon["new_tapchatthucte"] = phieudotapchat["new_tapchatthanhtoan"];
            }
            if (target.Contains("new_phieudoccs"))
            {
                Entity phieudoccs = service.Retrieve("new_phieudoccs", ((EntityReference)PostLenhDon["new_phieudoccs"]).Id, new ColumnSet(true));
                myLenhDon["new_ccsthucte"] = (decimal)phieudoccs["new_ccsthanhtoan"];
            }

            if (myLenhDon.Attributes.Count > 0)
            {
                service.Update(myLenhDon);

                if (PostLenhDon.Contains("new_phieucanvao") && PostLenhDon.Contains("new_phieucanra") && PostLenhDon.Contains("new_phieudotapchat") && PostLenhDon.Contains("new_phieudoccs"))
                {
                    Entity ctHD = service.Retrieve("new_thuadatcanhtac", ((EntityReference)PostLenhDon["new_thuacanhtac"]).Id, new ColumnSet(new string[] { "new_tinhtrangthuhoach", "new_sanluonguoc" }));

                    Entity chitietHD = new Entity("new_thuadatcanhtac");
                    chitietHD.Id = ((EntityReference)PostLenhDon["new_thuacanhtac"]).Id;

                    if (PostLenhDon.Contains("new_lenhdoncuoi") && (bool)PostLenhDon["new_lenhdoncuoi"])
                    {
                        chitietHD["new_tinhtrangthuhoach"] = new OptionSetValue(100000002);
                    }
                    else if (!ctHD.Contains("new_tinhtrangthuhoach") || ((OptionSetValue)ctHD["new_tinhtrangthuhoach"]).Value == 100000000)
                    {
                        chitietHD["new_tinhtrangthuhoach"] = new OptionSetValue(100000001);
                    }

                    if (!ctHD.Contains("new_tinhtrangthuhoach") || ((OptionSetValue)ctHD["new_tinhtrangthuhoach"]).Value == 100000000)
                    {
                        chitietHD["new_ngaythuhoachthucte"] = (PostLenhDon.Contains("new_thoigianchat") ? PostLenhDon["new_thoigianchat"] : null);
                    }

                    List<Entity> dslenhdon = RetrieveMultiRecord(service, "new_lenhdon", new ColumnSet(new string[] { "new_trongluongmia" }), "new_thuacanhtac", ((EntityReference)PostLenhDon["new_thuacanhtac"]).Id);
                    decimal totalsl = 0;
                    foreach (Entity a in dslenhdon)
                    {
                        totalsl += (a.Contains("new_trongluongmia") ? (decimal)a["new_trongluongmia"] : 0);
                    }

                    chitietHD["new_phantramtinhtrangthuhoach"] = (ctHD.Contains("new_sanluonguoc") ? (totalsl * 100 / (decimal)ctHD["new_sanluonguoc"]) : 0);
                    chitietHD["new_sanluongthucte"] = totalsl;

                    service.Update(chitietHD);
                }
            }
        }

        List<Entity> RetrieveMultiRecord(IOrganizationService crmservices, string entity, ColumnSet column, string condition, object value)
        {
            QueryExpression q = new QueryExpression(entity);
            q.ColumnSet = column;
            q.Criteria = new FilterExpression();
            q.Criteria.AddCondition(new ConditionExpression(condition, ConditionOperator.Equal, value));
            EntityCollection entc = service.RetrieveMultiple(q);

            return entc.Entities.ToList<Entity>();
        }

        List<Entity> FindChinhSachThuMua(IOrganizationService crmservices, Entity Lenhdon)
        {
            QueryExpression q = new QueryExpression("new_chinhsachthumua");
            q.ColumnSet = new ColumnSet(true);
            q.Criteria = new FilterExpression();
            q.Criteria.AddCondition(new ConditionExpression("new_vudautu", ConditionOperator.Equal, Lenhdon["new_vudautu"]));
            q.Criteria.AddCondition(new ConditionExpression("new_thoidiemapdung" , ConditionOperator.LessEqual, Lenhdon["CreatedOn"]));
            q.Criteria.AddCondition(new ConditionExpression("new_hoatdongapdung", ConditionOperator.Equal, new OptionSetValue(100000000)));
            EntityCollection entc = service.RetrieveMultiple(q);

            return entc.Entities.ToList<Entity>();
        }

    }
}
