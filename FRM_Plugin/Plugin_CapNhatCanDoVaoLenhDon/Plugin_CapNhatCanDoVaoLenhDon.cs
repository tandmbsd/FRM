using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk.Messages;

namespace Plugin_CapNhatCanDoVaoLenhDon
{
    public class Plugin_CapNhatCanDoVaoLenhDon : IPlugin
    {
        IOrganizationService service = null;
        IOrganizationServiceFactory factory = null;
        void IPlugin.Execute(IServiceProvider serviceProvider)
        {
            IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
            service = factory.CreateOrganizationService(context.UserId);

            Entity target = (Entity)context.InputParameters["Target"];

            if (target.LogicalName.Trim().ToLower() == "new_phieudangtai")
            {
                if (target.Contains("new_lenhdon"))
                {
                    Entity a = new Entity("new_lenhdon");
                    a.Id = ((EntityReference)target["new_lenhdon"]).Id;
                    a["new_phieudangtai"] = new EntityReference("new_phieudangtai", target.Id);
                    if (target.Contains("new_ngay"))
                        a["new_thoigiandangtai"] = target["new_ngay"];
                    if (target.Contains("new_loaimiachay"))
                        a["new_loaimiachay"] = target["new_loaimiachay"];

                    service.Update(a);
                }
            }
            else if (target.LogicalName.Trim().ToLower() == "new_phieucan")
            {
                if (target.Contains("new_lenhdon"))
                {
                    string query = " <fetch mapping='logical'> " +
                                 " <entity name='new_lenhdon'> " +
                                   " <attribute name='new_lenhdonid'/> " +
                                   " <link-entity name='new_phieucan' from='new_phieucanid' to='new_phieucanvao' link-type='outer' alias='a'> " +
                                     " <attribute name='new_trongluong' /> " +
                                   " </link-entity> " +
                                   " <link-entity name='new_phieucan' from='new_phieucanid' to='new_phieucanra' link-type='outer' alias='b'> " +
                                     " <attribute name='new_trongluong' /> " +
                                   " </link-entity> " +
                                   " <link-entity name='new_phieudotapchat' from='new_phieudotapchatid' to='new_phieudotapchat' link-type='outer' alias='c'> " +
                                     " <attribute name='new_tapchatthucte' /> " +
                                   " </link-entity> " +
                                   " <filter type='and'> " +
                                     " <condition attribute='new_lenhdonid' operator='eq' value='" + ((EntityReference)target["new_lenhdon"]).Id.ToString() + "' /> " +
                                   " </filter> " +
                                 " </entity> " +
                               " </fetch> ";
                    FetchExpression feq = new FetchExpression(query);
                    EntityCollection result = service.RetrieveMultiple(feq);

                    Entity a = new Entity("new_lenhdon");
                    a.Id = ((EntityReference)target["new_lenhdon"]).Id;
                    if (((OptionSetValue)target["new_loaican"]).Value == 100000000)
                    {
                        a["new_phieucanvao"] = new EntityReference("new_phieucan", target.Id);
                        if (target.Contains("new_maphieukiemdinh"))
                            a["new_phieukiemdinh"] = target["new_maphieukiemdinh"];
                        if (target.Contains("new_thoigiancanvao"))
                            a["new_thoigiancanvao"] = target["new_thoigiancanvao"];
                        if (target.Contains("new_trongluong"))
                        {
                            a["new_trongluongxoi"] = target["new_trongluong"];

                            if (result.Entities.Count > 0)
                                if (result[0].Contains("b.new_trongluong") && result[0].Contains("c.new_tapchatthucte"))
                                {
                                    a["new_trongluongbi"] = (decimal)((AliasedValue)result[0]["b.new_trongluong"]).Value;
                                    a["new_tapchatthucte"] = (decimal)((AliasedValue)result[0]["c.new_tapchatthucte"]).Value;

                                    decimal tlmia = (decimal)target["new_trongluong"] - (decimal)((AliasedValue)result[0]["b.new_trongluong"]).Value;
                                    a["new_trongluongmia"] = tlmia * (1 - ((decimal)((AliasedValue)result[0]["c.new_tapchatthucte"]).Value / 100));
                                }
                        }

                        service.Update(a);
                    }
                    else
                    {
                        a["new_phieucanra"] = new EntityReference("new_phieucan", target.Id);
                        if (target.Contains("new_thoigiancanvao"))
                            a["new_thoigiancanra"] = target["new_thoigiancanvao"];
                        if (target.Contains("new_trongluong"))
                        {
                            a["new_trongluongbi"] = target["new_trongluong"];

                            if (result.Entities.Count > 0)
                                if (result[0].Contains("a.new_trongluong") && result[0].Contains("c.new_tapchatthucte"))
                                {
                                    a["new_trongluongxoi"] = (decimal)((AliasedValue)result[0]["a.new_trongluong"]).Value;
                                    a["new_tapchatthucte"] = (decimal)((AliasedValue)result[0]["c.new_tapchatthucte"]).Value;

                                    decimal tlmia = (decimal)((AliasedValue)result[0]["a.new_trongluong"]).Value - (decimal)target["new_trongluong"];
                                    a["new_trongluongmia"] = tlmia * (1 - ((decimal)((AliasedValue)result[0]["c.new_tapchatthucte"]).Value / 100));
                                }
                        }
                        a["statuscode"] = new OptionSetValue(100000002);
                        service.Update(a);
                    }
                }
            }
            else if (target.LogicalName.Trim().ToLower() == "new_phieudoccs")
            {
                if (target.Contains("new_lenhdon"))
                {
                    Entity a = new Entity("new_lenhdon");
                    a.Id = ((EntityReference)target["new_lenhdon"]).Id;
                    a["new_phieudoccs"] = new EntityReference("new_phieudoccs", target.Id);
                    if (target.Contains("new_ccsthanhtoan"))
                        a["new_ccsthucte"] = target["new_ccsthanhtoan"];
                    if (target.Contains("new_ngay"))
                        a["new_thoigiandoccs"] = target["new_ngay"];
                    service.Update(a);
                }
            }
            else if (target.LogicalName.Trim().ToLower() == "new_phieudotapchat")
            {
                if (target.Contains("new_lenhdon"))
                {
                    string query = " <fetch mapping='logical'> " +
                                 " <entity name='new_lenhdon'> " +
                                   " <attribute name='new_lenhdonid'/> " +
                                   " <link-entity name='new_phieucan' from='new_phieucanid' to='new_phieucanvao' link-type='outer' alias='a'> " +
                                     " <attribute name='new_trongluong' /> " +
                                   " </link-entity> " +
                                   " <link-entity name='new_phieucan' from='new_phieucanid' to='new_phieucanra' link-type='outer' alias='b'> " +
                                     " <attribute name='new_trongluong' /> " +
                                   " </link-entity> " +
                                   " <link-entity name='new_phieudotapchat' from='new_phieudotapchatid' to='new_phieudotapchat' link-type='outer' alias='c'> " +
                                     " <attribute name='new_tapchatthucte' /> " +
                                   " </link-entity> " +
                                   " <filter type='and'> " +
                                     " <condition attribute='new_lenhdonid' operator='eq' value='" + ((EntityReference)target["new_lenhdon"]).Id.ToString() + "' /> " +
                                   " </filter> " +
                                 " </entity> " +
                               " </fetch> ";
                    FetchExpression feq = new FetchExpression(query);
                    EntityCollection result = service.RetrieveMultiple(feq);

                    Entity a = new Entity("new_lenhdon");
                    a.Id = ((EntityReference)target["new_lenhdon"]).Id;
                    a["new_phieudotapchat"] = new EntityReference("new_phieudotapchat", target.Id);
                    if (target.Contains("new_ngay"))
                        a["new_thoigiandotapchat"] = target["new_ngay"];
                    if (target.Contains("new_tapchatthucte"))
                    {
                        a["new_tapchatthucte"] = target["new_tapchatthucte"];

                        if (result.Entities.Count > 0)
                            if (result[0].Contains("a.new_trongluong") && result[0].Contains("b.new_trongluong"))
                            {
                                a["new_trongluongxoi"] = (decimal)((AliasedValue)result[0]["a.new_trongluong"]).Value;
                                a["new_trongluongbi"] = (decimal)((AliasedValue)result[0]["b.new_trongluong"]).Value;

                                decimal tlmia = (decimal)((AliasedValue)result[0]["a.new_trongluong"]).Value - (decimal)((AliasedValue)result[0]["b.new_trongluong"]).Value;
                                a["new_trongluongmia"] = tlmia * (1 - ((decimal)target["new_tapchatthucte"] / 100));
                            }
                    }
                    service.Update(a);
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
    }
}
