using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;

namespace Plugin_ChangStatusHDOnThamDinh
{
    public class Plugin_ChangStatusHDOnThamDinh : IPlugin
    {
        IOrganizationService service = null;
        IOrganizationServiceFactory factory = null;
        void IPlugin.Execute(IServiceProvider serviceProvider)
        {
            IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
            service = factory.CreateOrganizationService(context.UserId);

            Entity target = (Entity)context.InputParameters["Target"];
            Entity fullEntity = null;
            Entity PreImg = null;
            bool check = false;
            if (context.MessageName == "Create")
            {
                check = true;
                fullEntity = target;
                PreImg = target;
            }
            else if (target.Contains("new_hopdongdautumia") || target.Contains("new_hopdongdaututhuedat"))
            {
                fullEntity = (Entity)context.PostEntityImages["PostImg"];
                PreImg = (Entity)context.PreEntityImages["PreImg"];
                if (fullEntity.Contains("new_tinhtrangduyet") && ((OptionSetValue)fullEntity["new_tinhtrangduyert"]).Value == 100000000)
                    if (fullEntity.Contains("statuscode") && ((OptionSetValue)fullEntity["statuscode"]).Value == 1)
                        check = true;

                if (fullEntity.Contains("statuscode") && ((OptionSetValue)fullEntity["statuscode"]).Value == 100000001)
                {
                    if (PreImg.Contains("new_hopdongdautumia"))
                    {
                        Entity up = new Entity("new_hopdongdautumia");
                        up.Id = ((EntityReference)PreImg["new_hopdongdautumia"]).Id;
                        up["statuscode"] = new OptionSetValue(1);
                        service.Update(up);

                        QueryExpression q = new QueryExpression("new_thuadatcanhtac");
                        q.ColumnSet = new ColumnSet("new_thuadatcanhtacid", "statuscode");
                        q.Criteria.AddCondition(new ConditionExpression("new_hopdongdautumia",ConditionOperator.Equal , ((EntityReference)PreImg["new_hopdongdautumia"]).Id));

                        EntityCollection result = service.RetrieveMultiple(q);

                        foreach(Entity a in result.Entities)
                        {
                            if (((OptionSetValue)a["statuscode"]).Value == 100000008)
                            {
                                Entity b = new Entity("new_thuadatcanhtac");
                                b["statuscode"] = new OptionSetValue(1);
                                b.Id = a.Id;
                                service.Update(b);
                            }
                        }
                    }

                    if (PreImg.Contains("new_hopdongdaututhuedat"))
                    {
                        Entity up = new Entity("new_hopdongthuedat");
                        up.Id = ((EntityReference)PreImg["new_hopdongdaututhuedat"]).Id;
                        up["statuscode"] = new OptionSetValue(1);
                        service.Update(up);

                        QueryExpression q = new QueryExpression("new_chitiethdthuedat_thuadat");
                        q.ColumnSet = new ColumnSet("new_chitiethdthuedat_thuadatid", "statuscode");
                        LinkEntity l1 = new LinkEntity("new_chitiethdthuedat_thuadat", "new_datthue", "new_chitiethdthuedat", "new_datthueid", JoinOperator.Inner);
                        l1.LinkCriteria.AddCondition(new ConditionExpression("new_hopdongthuedat", ConditionOperator.Equal, ((EntityReference)PreImg["new_hopdongdaututhuedat"]).Id));
                        q.LinkEntities.Add(l1);

                        EntityCollection result = service.RetrieveMultiple(q);
                        foreach (Entity a in result.Entities)
                        {
                            if (((OptionSetValue)a["statuscode"]).Value == 100000008)
                            {
                                Entity b = new Entity("new_chitiethdthuedat_thuadat");
                                b["statuscode"] = new OptionSetValue(1);
                                b.Id = a.Id;
                                service.Update(b);
                            }
                        }
                    }
                }
            }
            #region Chay khi tinh trang nhap.
            if (check)
            {
                if (target.Contains("new_hopdongdautumia"))
                {
                    Entity HDMia = service.Retrieve("new_hopdongdautumia", ((EntityReference)target["new_hopdongdautumia"]).Id, new ColumnSet(new string[] { "statuscode" }));
                    if (((OptionSetValue)HDMia["statuscode"]).Value != 1)
                        throw new InvalidPluginExecutionException("Hợp đồng đầu tư mía bạn chọn tình trạng không phù hợp, vui lòng chọn HĐ khác để thẩm định !");
                    else
                    {
                        Entity up = new Entity("new_hopdongdautumia");
                        up.Id = HDMia.Id;
                        up["statuscode"] = new OptionSetValue(100000005);
                        service.Update(up);
                    }
                    if (context.MessageName == "Update" && PreImg.Contains("new_hopdongdautumia"))
                    {
                        Entity up = new Entity("new_hopdongdautumia");
                        up.Id = ((EntityReference)PreImg["new_hopdongdautumia"]).Id;
                        up["statuscode"] = new OptionSetValue(1);
                        service.Update(up);
                    }

                    StringBuilder fetchXML = new StringBuilder();
                    fetchXML.AppendFormat("<fetch mapping='logical' distinct='false' aggregate='true' version='1.0'>");
                    fetchXML.AppendFormat("<entity name='new_thuadatcanhtac'>");
                    fetchXML.AppendFormat("<attribute name='new_dientichhopdong' aggregate='sum' alias='v_new_dientichhopdong'/>");
                    fetchXML.AppendFormat("<attribute name='new_loaigocmia' alias='new_loaigocmia' groupby='true'/>");
                    fetchXML.AppendFormat("<attribute name='statuscode' alias='statuscode' groupby='true'/>");
                    fetchXML.AppendFormat("<filter type='and'>");
                    fetchXML.AppendFormat("<condition attribute='new_hopdongdautumia' operator='eq' value='{0}' />", HDMia.Id.ToString());
                    fetchXML.AppendFormat("<condition attribute='statecode' operator='eq' value='{0}' />", 0);
                    fetchXML.AppendFormat("</filter>");
                    fetchXML.AppendFormat("</entity>");
                    fetchXML.AppendFormat("</fetch>");

                    decimal Dtotal = 0;
                    decimal Dt_to = 0;
                    decimal Dt_goc = 0;

                    decimal Ktotal = 0;
                    decimal Kt_to = 0;
                    decimal Kt_goc = 0;

                    EntityCollection result = service.RetrieveMultiple(new FetchExpression(fetchXML.ToString()));
                    if (result.Entities.Count > 0)
                    {
                        foreach (Entity a in result.Entities)
                        {
                            if (((OptionSetValue)((AliasedValue)a["statuscode"]).Value).Value == 1)
                            {
                                if (((OptionSetValue)((AliasedValue)a["new_loaigocmia"]).Value).Value == 100000000)
                                {
                                    Dtotal += (((AliasedValue)a["v_new_dientichhopdong"]).Value == null ? 0 : (decimal)((AliasedValue)a["v_new_dientichhopdong"]).Value);
                                    Dt_to += (((AliasedValue)a["v_new_dientichhopdong"]).Value == null ? 0 : (decimal)((AliasedValue)a["v_new_dientichhopdong"]).Value);
                                }
                                else
                                {
                                    Dtotal += (((AliasedValue)a["v_new_dientichhopdong"]).Value == null ? 0 : (decimal)((AliasedValue)a["v_new_dientichhopdong"]).Value);
                                    Dt_goc += (((AliasedValue)a["v_new_dientichhopdong"]).Value == null ? 0 : (decimal)((AliasedValue)a["v_new_dientichhopdong"]).Value);
                                }
                            }
                            else if(((OptionSetValue)((AliasedValue)a["statuscode"]).Value).Value == 100000008)
                            {
                                if (((OptionSetValue)((AliasedValue)a["new_loaigocmia"]).Value).Value == 100000000)
                                {
                                    Ktotal += (((AliasedValue)a["v_new_dientichhopdong"]).Value == null ? 0 : (decimal)((AliasedValue)a["v_new_dientichhopdong"]).Value);
                                    Kt_to += (((AliasedValue)a["v_new_dientichhopdong"]).Value == null ? 0 : (decimal)((AliasedValue)a["v_new_dientichhopdong"]).Value);
                                }
                                else
                                {
                                    Ktotal += (((AliasedValue)a["v_new_dientichhopdong"]).Value == null ? 0 : (decimal)((AliasedValue)a["v_new_dientichhopdong"]).Value);
                                    Kt_goc += (((AliasedValue)a["v_new_dientichhopdong"]).Value == null ? 0 : (decimal)((AliasedValue)a["v_new_dientichhopdong"]).Value);
                                }
                            }
                        }
                    }

                    Entity PTD = new Entity("new_phieuthamdinhdautu");
                    PTD.Id = target.Id;
                    PTD["new_tongdientich_chapthuan"] = Dtotal;
                    PTD["new_trongmoi_chapthuan"] = Dt_to;
                    PTD["new_chamsocgoc_chapthuan"] = Dt_goc;
                    PTD["new_tongdientich_khongchapthuan"] = Ktotal;
                    PTD["new_trongmoi_khongchapthuan"] = Kt_to;
                    PTD["new_chamsocgoc_khongchapthuan"] = Kt_goc;
                    service.Update(PTD);

                }

                if (target.Contains("new_hopdongdaututhuedat"))
                {
                    Entity HDthuedat = service.Retrieve("new_hopdongthuedat", ((EntityReference)target["new_hopdongdaututhuedat"]).Id, new ColumnSet(new string[] { "statuscode" }));
                    if (((OptionSetValue)HDthuedat["statuscode"]).Value != 1)
                        throw new InvalidPluginExecutionException("Hợp đồng đầu tư thuê đất bạn chọn tình trạng không phù hợp, vui lòng chọn HĐ khác để thẩm định !");
                    else
                    {
                        Entity up = new Entity("new_hopdongthuedat");
                        up.Id = HDthuedat.Id;
                        up["statuscode"] = new OptionSetValue(100000004);
                        service.Update(up);
                    }
                    if (context.MessageName == "Update" && PreImg.Contains("new_hopdongdaututhuedat"))
                    {
                        Entity up = new Entity("new_hopdongthuedat");
                        up.Id = ((EntityReference)PreImg["new_hopdongdaututhuedat"]).Id;
                        up["statuscode"] = new OptionSetValue(1);
                        service.Update(up);
                    }
                }

            }
            #endregion

        }
    }
}
