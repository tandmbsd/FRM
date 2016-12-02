using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk.Messages;
using System.Web.Script.Serialization;

namespace Plugin_QuotaDuyetLenhDon
{
    public class Plugin_QuotaDuyetLenhDon : IPlugin
    {
        IOrganizationService service = null;
        IOrganizationServiceFactory factory = null;
        void IPlugin.Execute(IServiceProvider serviceProvider)
        {
            IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
            service = factory.CreateOrganizationService(context.UserId);
            ITracingService trace = (ITracingService)serviceProvider.GetService(typeof(ITracingService));
            Entity target = context.InputParameters["Target"] as Entity;

            if (target.Contains("statuscode") && ((OptionSetValue)target["statuscode"]).Value == 100000000)
            {
                Entity FullEntity = (Entity)context.PostEntityImages["PostImg"];
                //get quota
                QueryExpression qe = new QueryExpression("new_quotacaplenhdon");
                qe.ColumnSet = new ColumnSet(new string[] { "new_quotacaplenhdonid" });
                qe.Criteria.Conditions.Add(new ConditionExpression("new_ngayapdung", ConditionOperator.On, (DateTime)FullEntity["new_ngaycap"]));
                LinkEntity l1 = new LinkEntity("new_quotacaplenhdon", "new_vuthuhoach", "new_vuthuhoach", "new_vuthuhoachid", JoinOperator.Inner);
                l1.LinkCriteria.Conditions.Add(new ConditionExpression("new_vudautu", ConditionOperator.Equal, ((EntityReference)FullEntity["new_vudautu"]).Id));
                qe.LinkEntities.Add(l1);
                LinkEntity l2 = new LinkEntity("new_quotacaplenhdon", "new_chitietquotacaplenhdon", "new_quotacaplenhdonid", "new_quotacaplenhdon", JoinOperator.Inner);
                l2.LinkCriteria.Conditions.Add(new ConditionExpression("new_tram", ConditionOperator.Equal, ((EntityReference)FullEntity["new_tram"]).Id));
                l2.Columns = new ColumnSet("new_soluong");
                l2.EntityAlias = "a";
                qe.LinkEntities.Add(l2);

                EntityCollection result = service.RetrieveMultiple(qe);
                if (result.Entities.Count > 0) //có quota
                {
                    decimal quota = (result[0].Contains("a.new_soluong") ? (decimal)((AliasedValue)result[0]["a.new_soluong"]).Value : (decimal)0);
                    if (quota <= 0)
                        throw new Exception(string.Format("Quota ngày {0} của trạm {1} đã hết, bạn vui lòng cấp ngày khác!",
                        ((DateTime)FullEntity["new_ngaycap"]).AddHours(7).ToString("dd/MM/yyyy"), ((EntityReference)FullEntity["new_tram"]).Name));
                    else
                    {
                        string fetch = string.Format(@"<fetch distinct='false' mapping='logical' aggregate='true'>
                                                      <entity name='new_lenhdon'>
                                                        <attribute name='new_lenhdonid' alias='new_lenhdon_count' aggregate='count'/>
                                                        <filter type='and'>
                                                          <condition attribute='new_lenhdonid' operator='neq' value='{0}' />
                                                          <condition attribute='new_ngaycap' operator='on' value='{1}' />
                                                          <condition attribute='new_tram' operator='eq' value='{2}' />
                                                          <condition attribute='new_vudautu' operator='eq' value='{3}' />
                                                          <condition attribute='statuscode' operator='in'>
                                                            <value>100000000</value>
                                                            <value>100000002</value>
                                                            <value>100000003</value>
                                                          </condition>
                                                        </filter>
                                                      </entity>
                                                    </fetch>"
                        , FullEntity.Id.ToString(), ((DateTime)FullEntity["new_ngaycap"]).AddHours(7).ToString("yyyy-MM-dd"), ((EntityReference)FullEntity["new_tram"]).Id.ToString(), ((EntityReference)FullEntity["new_vudautu"]).Id.ToString());
                        RetrieveMultipleRequest fetchRequest1 = new RetrieveMultipleRequest
                        {
                            Query = new FetchExpression(fetch)
                        };

                        EntityCollection returnCollection = ((RetrieveMultipleResponse)service.Execute(fetchRequest1)).EntityCollection;
                        if (returnCollection.Entities.Count > 0 && returnCollection[0].Contains("new_lenhdon_count"))
                        {
                            int kq = (int)((AliasedValue)returnCollection[0]["new_lenhdon_count"]).Value;
                            if (kq >= quota)
                                throw new Exception(string.Format("Quota ngày {0} của trạm {1} đã hết, bạn vui lòng cấp ngày khác!",
                                ((DateTime)FullEntity["new_ngaycap"]).AddHours(7).ToString("dd/MM/yyyy"), ((EntityReference)FullEntity["new_tram"]).Name));
                            else //check update uu tien
                            {
                                QueryExpression qek = new QueryExpression("new_xeuutien");
                                qek.ColumnSet = new ColumnSet("new_loaiuutien", "new_soxe", "new_tram", "new_chumiakhcn", "new_chumiakhdn");
                                qek.Criteria.Conditions.Add(new ConditionExpression("new_tungay", ConditionOperator.OnOrBefore, (DateTime)FullEntity["new_ngaycap"]));
                                qek.Criteria.Conditions.Add(new ConditionExpression("new_denngay", ConditionOperator.OnOrAfter, (DateTime)FullEntity["new_ngaycap"]));
                                qek.Criteria.Conditions.Add(new ConditionExpression("new_vudautu", ConditionOperator.Equal, ((EntityReference)FullEntity["new_vudautu"]).Id));

                                EntityCollection dsut = service.RetrieveMultiple(qek);
                                bool co = false;
                                foreach (Entity a in dsut.Entities)
                                {
                                    if (((OptionSetValue)a["new_loaiuutien"]).Value == 100000000)
                                    {
                                        if (((EntityReference)FullEntity["new_tram"]).Id == ((EntityReference)a["new_tram"]).Id)
                                        {
                                            co = true;
                                            break;
                                        }
                                    }
                                    else if (((OptionSetValue)a["new_loaiuutien"]).Value == 100000001) //theo chủ mía
                                    {
                                        if (FullEntity.Contains("new_khachhang") && a.Contains("new_chumiakhcn"))
                                        {
                                            if (((EntityReference)FullEntity["new_khachhang"]).Id == ((EntityReference)FullEntity["new_chumiakhcn"]).Id)
                                            {
                                                co = true;
                                                break;
                                            }
                                        }
                                        else if (FullEntity.Contains("new_khachhangdoanhnghiep") && a.Contains("new_chumiakhdn"))
                                        {
                                            if (((EntityReference)FullEntity["new_khachhangdoanhnghiep"]).Id == ((EntityReference)FullEntity["new_chumiakhdn"]).Id)
                                            {
                                                co = true;
                                                break;
                                            }
                                        }
                                    }
                                    else //theo xe
                                    {
                                        if (FullEntity.Contains("new_xevanchuyen") && ((EntityReference)FullEntity["new_xevanchuyen"]).Id == ((EntityReference)FullEntity["new_soxe"]).Id)
                                        {
                                            co = true;
                                            break;
                                        }
                                    }
                                }
                                if (co)
                                {
                                    Entity up = new Entity("new_lenhdon");
                                    up.Id = target.Id;
                                    up["new_uutien"] = true;
                                    service.Update(up);
                                }
                            }
                        }
                    }
                }
                else
                    throw new Exception(string.Format("Quota ngày {0} của trạm {1} chưa được cấp, vui lòng báo về phòng Nông Nghiệp để hỗ trợ !",
                        ((DateTime)FullEntity["new_ngaycap"]).AddHours(7).ToString("dd/MM/yyyy"), ((EntityReference)FullEntity["new_tram"]).Name
                        ));
            }
        }
    }
}
