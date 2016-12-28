using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace UpdatePBDTFromPTL
{
    public class UpdatePBDTFromPTL : IPlugin
    {
        IOrganizationService service = null;
        IOrganizationServiceFactory factory = null;
        ITracingService trace = null;

        void IPlugin.Execute(IServiceProvider serviceProvider)
        {
            IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
            trace = (ITracingService)serviceProvider.GetService(typeof(ITracingService));
            service = factory.CreateOrganizationService(context.UserId);

            Entity target = (Entity)context.InputParameters["Target"];
            decimal datragoc = 0;
            decimal datralai = 0;

            DateTime ngayvay = new DateTime();

            if (target.Contains("new_tinhtrangduyet") && ((OptionSetValue)target["new_tinhtrangduyet"]).Value == 100000006) // da duyet
            {
                //EntityCollection lstPhieutinhlai = RetrieveNNRecord(service, "new_phieutinhlai", "new_phieudenghithuno",
                //"new_new_phieudenghithuno_new_phanbodautu", new ColumnSet(true), "new_phieudenghithunoid", target.Id);

                List<Entity> lstPhieutinhlai = RetrieveMultiRecord(service, "new_phieutinhlai",
                    new ColumnSet(true), "new_phieudenghithuno", target.Id);

                foreach (Entity en in lstPhieutinhlai)
                {
                    Entity pbdt = service.Retrieve("new_phanbodautu", ((EntityReference)en["new_phanbodautu"]).Id,
                        new ColumnSet(new string[] { "new_datra", "new_datralai", "new_nolai",
                            "new_conlai", "new_sotien", "new_ngaytinhlaisaucung" }));

                    datragoc = en.Contains("new_tienvay") ? ((Money)en["new_tienvay"]).Value : new decimal(0);
                    datralai = en.Contains("new_tienlai") ? ((Money)en["new_tienlai"]).Value : new decimal(0);

                    decimal sotienPBDT = pbdt.Contains("new_sotien") ? ((Money)pbdt["new_sotien"]).Value : new decimal(0);
                    decimal datragocPBDT = pbdt.Contains("new_datra") ? ((Money)pbdt["new_datra"]).Value : new decimal(0);
                    decimal datralaiPBDT = pbdt.Contains("new_datralai") ? ((Money)pbdt["new_datralai"]).Value : new decimal(0);
                    decimal nolaiPBDt = pbdt.Contains("new_nolai") ? ((Money)pbdt["new_nolai"]).Value : new decimal(0);
                    decimal conlaiPBDT = pbdt.Contains("new_conlai") ? ((Money)pbdt["new_conlai"]).Value : new decimal(0);

                    if (en.Contains("new_phieudenghithuno"))
                    {
                        Entity pdnthuno = service.Retrieve("new_phieudenghithuno", ((EntityReference)en["new_phieudenghithuno"]).Id,
                            new ColumnSet(new string[] { "new_ngaythu" }));
                        ngayvay = (DateTime)pdnthuno["new_ngaythu"];
                    }

                    datragocPBDT += datragoc;
                    datralaiPBDT += datralai;
                    nolaiPBDt += 0;
                    conlaiPBDT = sotienPBDT - datragocPBDT;

                    trace.Trace(sotienPBDT.ToString() + "-" + datragocPBDT.ToString());

                    pbdt["new_datra"] = new Money(datragocPBDT);
                    pbdt["new_datralai"] = new Money(datralaiPBDT);
                    pbdt["new_nolai"] = new Money(0);
                    pbdt["new_conlai"] = new Money(conlaiPBDT);
                    pbdt["new_ngaytinhlaisaucung"] = ngayvay;

                    service.Update(pbdt);
                }

                #region cập nhập công nợ KH

                Entity pdntn = service.Retrieve(target.LogicalName, target.Id,
                    new ColumnSet(new string[] { "new_khachhang", "new_khachhangdoanhnghiep", "new_thunogoc" }));

                Entity KH = null;

                if (pdntn.Contains("new_khachhang"))
                    KH = service.Retrieve("contact", ((EntityReference)pdntn["new_khachhang"]).Id,
                        new ColumnSet(new string[] { "new_co", "new_no", "new_duno" }));

                else if (pdntn.Contains("new_khachhangdoanhnghiep"))
                    KH = service.Retrieve("account", ((EntityReference)pdntn["new_khachhang"]).Id,
                        new ColumnSet(new string[] { "new_co", "new_no", "new_duno" }));


                decimal tongtiendautu = KH.Contains("new_co") ? ((Money)KH["new_co"]).Value : new decimal(0);
                decimal thunogoc = pdntn.Contains("new_thunogoc") ? ((Money)pdntn["new_thunogoc"]).Value : new decimal(0);
                decimal tiendatra = (KH.Contains("new_no") ? ((Money)KH["new_no"]).Value : new decimal(0)) + thunogoc;
                decimal conlai = tongtiendautu - tiendatra;

                KH["new_duno"] = new Money(conlai);
                KH["new_no"] = new Money(tiendatra);
                service.Update(KH);

                #endregion
            }
        }
        EntityCollection RetrieveNNRecord(IOrganizationService crmservices, string entity1, string entity2, string relateName, ColumnSet column, string condition, object value)
        {
            EntityReferenceCollection result = new EntityReferenceCollection();
            QueryExpression query = new QueryExpression(entity1);
            query.ColumnSet = column;
            LinkEntity linkEntity1 = new LinkEntity(entity1, relateName, entity1 + "id", entity1 + "id", JoinOperator.Inner);
            LinkEntity linkEntity2 = new LinkEntity(relateName, entity2, entity2 + "id", entity2 + "id", JoinOperator.Inner);

            linkEntity1.LinkEntities.Add(linkEntity2);
            query.LinkEntities.Add(linkEntity1);

            linkEntity2.LinkCriteria = new FilterExpression();
            linkEntity2.LinkCriteria.AddCondition(new ConditionExpression(condition, ConditionOperator.Equal, value));
            query.AddOrder("new_ngayvay", OrderType.Ascending);
            EntityCollection collRecords = service.RetrieveMultiple(query);

            return collRecords;
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
