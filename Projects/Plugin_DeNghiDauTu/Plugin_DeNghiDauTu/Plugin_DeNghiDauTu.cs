using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk.Messages;
using Microsoft.Crm.Sdk.Messages;

namespace Plugin_DeNghiDauTu
{
    public class Plugin_DeNghiDauTu : IPlugin
    {
        IOrganizationService service = null;
        IOrganizationServiceFactory factory = null;
        void IPlugin.Execute(IServiceProvider serviceProvider)
        {
            IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            ITracingService traceService = (ITracingService)serviceProvider.GetService(typeof(ITracingService));
            Entity target = (Entity)context.InputParameters["Target"];
            factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
            service = factory.CreateOrganizationService(context.UserId);
            int depth = context.Depth;

            if (target.Contains("statuscode") && ((OptionSetValue)target["statuscode"]).Value == 100000002)
            {
                string vl = "";
                Entity Opp = service.Retrieve(target.LogicalName, target.Id, new ColumnSet(true));

                //if (((OptionSetValue)Opp["statuscode"]).Value == 3)
                //{
                string[] loaihd = Opp["new_loaihopdong_vl"].ToString().Split(',');
                vl = Opp["new_loaihopdong_vl"].ToString();

                foreach (string a in loaihd)
                {
                    Entity Hd = new Entity();
                    Hd["new_name"] = Opp.Contains("name") ? Opp["name"] : null;
                    Hd["new_vudautu"] = Opp.Contains("new_vudautu") ? Opp["new_vudautu"] : null;
                    Hd["new_cohoi"] = new EntityReference(Opp.LogicalName, Opp.Id);
                    Hd["new_tram"] = Opp.Contains("new_tram") ? Opp["new_tram"] : null;
                    Hd["new_canbonongvu"] = Opp.Contains("new_canbonongvu") ? Opp["new_canbonongvu"] : null;
                    Hd["transactioncurrencyid"] = Opp["transactioncurrencyid"];
                    Guid Hdid;

                    if (a == "100000000")
                    { //Hợp đồng đầu tư thuê đất
                        Hd.LogicalName = "new_hopdongthuedat";
                        if (((EntityReference)Opp["customerid"]).LogicalName == "contact")
                        {
                            Hd["new_khachhang"] = Opp["customerid"];
                        }
                        else
                        {
                            Hd["new_khachhangdoanhnghiep"] = Opp["customerid"];
                        }
                        Hdid = service.Create(Hd);
                    }
                    else if (a == "100000001")
                    { //Hợp đồng thế chấp
                        Hd.LogicalName = "new_hopdongthechap";
                        if (((EntityReference)Opp["customerid"]).LogicalName == "contact")
                        {
                            Hd["new_chuhopdong"] = Opp["customerid"];
                        }
                        else
                        {
                            Hd["new_chuhopdongdoanhnghiep"] = Opp["customerid"];
                        }
                        Hdid = service.Create(Hd);

                        List<Entity> lstTaisanthechap = RetrieveMultiRecord(service, "new_chitietcohoi_taisanthechap", new ColumnSet(true), "new_cohoi", Opp.Id);

                        foreach (Entity en in lstTaisanthechap)
                        {
                            Entity taisan = new Entity("new_taisan");
                            taisan["new_name"] = en["new_name"];
                            taisan["new_giatridinhgiagiatrithechap"] = ((Money)en["new_dongia"]).Value * (int)en["new_soluong"];
                            Guid taisanID = service.Create(taisan);

                            Entity newHopdongthechap = new Entity("new_hopdongthechap");
                            newHopdongthechap["new_vudautu"] = Opp["new_vudautu"];
                            if (Opp.Contains("new_khachhang"))
                            {
                                newHopdongthechap["new_chuhopdong"] = Opp["new_khachhang"];
                            }
                            if (Opp.Contains("new_khachhangdoanhnghiep"))
                            {
                                newHopdongthechap["new_chuhopdongdoanhnghiep"] = Opp["new_khachhangdoanhnghiep"];
                            }
                            newHopdongthechap["new_tram"] = Opp["new_tram"];
                            newHopdongthechap["new_canbonongvu"] = Opp["new_canbonongvu"];

                            Guid hdid = service.Create(newHopdongthechap);

                            Entity taisanthechap = new Entity("new_taisanthechap");
                            taisanthechap["new_hopdongthechap"] = new EntityReference(newHopdongthechap.LogicalName, hdid);
                            taisanthechap["new_taisan"] = new EntityReference(taisan.LogicalName, taisanID);

                            service.Create(taisanthechap);

                        }
                    }
                    else if (a == "100000002")
                    { //Hợp đồng đầu tư mía
                        Hd.LogicalName = "new_hopdongdautumia";

                        if (((EntityReference)Opp["customerid"]).LogicalName == "contact")
                        {
                            Hd["new_khachhang"] = Opp["customerid"];
                        }
                        else
                        {
                            Hd["new_khachhangdoanhnghiep"] = Opp["customerid"];
                        }

                        if (((OptionSetValue)Opp["new_loaihopdongdautumia"]).Value >= 100000000 && ((OptionSetValue)Opp["new_loaihopdongdautumia"]).Value <= 100000002)
                        {
                            Hd["new_loaihopdong"] = Opp["new_loaihopdongdautumia"];
                        }

                        Hdid = service.Create(Hd);

                        List<Entity> lstChitietcohoi = RetrieveMultiRecord(service, "new_chitietcohoi_dientichhopdong", new ColumnSet(true), "new_cohoi", Opp.Id);

                        foreach (Entity en in lstChitietcohoi)
                        {
                            Entity thuadat = new Entity("new_thuadat");

                            thuadat["new_dientich"] = en["new_dientichmia"];
                            thuadat["new_vungdialy"] = en["new_vungdialy"];
                            thuadat["new_nhomdat"] = en.Contains("new_nhomdat") ? en["new_nhomdat"] : null;

                            Guid thuadatID = service.Create(thuadat);

                            Entity tdct = new Entity("new_thuadatcanhtac");

                            if (((EntityReference)Opp["customerid"]).LogicalName == "contact")
                                tdct["new_khachhang"] = Opp["customerid"];
                            else
                                tdct["new_khachhangdoanhnghiep"] = Opp["customerid"];

                            Entity newthuadat = service.Retrieve(thuadat.LogicalName, thuadatID, new ColumnSet(new string[] { "new_name" }));
                            tdct["new_name"] = (Opp.Contains("new_vungdialy") ? Opp["new_vungdialy"].ToString() : "")
                                + "-" + newthuadat["new_name"];
                            tdct["new_loaitrong"] = en.Contains("new_loaitrong") ? en["new_loaitrong"] : null;
                            tdct["new_ngaytrongdukien"] = en.Contains("new_ngaytrongdukien") ? en["new_loaitrong"] : DateTime.Now;
                            tdct["new_vutrong"] = en.Contains("new_vutrong") ? en["new_vutrong"] : null;
                            tdct["new_loaigocmia"] = en.Contains("new_loaigocmia") ? en["new_loaigocmia"] : null;
                            tdct["new_giongmia"] = en.Contains("new_giongmia") ? en["new_giongmia"] : null;
                            //tdct["new_sanluongtheolythuyet"] = en.Contains("new_sanluong") ? en["new_sanluong"] : 0;
                            //tdct["new_nangsuatlythuyet"] = en.Contains("new_nangsuat") ? en["new_nangsuat"] : 0;                                
                            tdct["new_thuadat"] = new EntityReference(thuadat.LogicalName, thuadatID);
                            tdct["new_hopdongdautumia"] = new EntityReference(Hd.LogicalName, Hdid);
                            tdct["new_dinhmucdautuhoanlai"] = en.Contains("new_dinhmuc") ? new Money(((Money)en["new_dinhmuc"]).Value) : new Money(0);
                            tdct

                            service.Create(tdct);
                        }
                    }
                    else if (a == "100000003")
                    {//Hợp đồng đầu tư trang thiết bị
                        Hd.LogicalName = "new_hopdongdaututrangthietbi";
                        if (((EntityReference)Opp["customerid"]).LogicalName == "contact")
                        {
                            Hd["new_doitaccungcap"] = Opp["customerid"];
                        }
                        else
                        {
                            Hd["new_doitaccungcapkhdn"] = Opp["customerid"];
                        }
                        Hdid = service.Create(Hd);


                    }
                    else if (a == "100000004")
                    { //Hợp đồng thu hoạch
                        Hd.LogicalName = "new_hopdongthuhoach";
                        if (((EntityReference)Opp["customerid"]).LogicalName == "contact")
                        {
                            Hd["new_doitacthuhoach"] = Opp["customerid"];
                        }
                        else
                        {
                            Hd["new_doitacthuhoachkhdn"] = Opp["customerid"];
                        }
                        Hdid = service.Create(Hd);
                    }
                    else if (a == "100000005")
                    { //Hợp đồng vận chuyển
                        Hd.LogicalName = "new_hopdongvanchuyen";
                        if (((EntityReference)Opp["customerid"]).LogicalName == "contact")
                        {
                            Hd["new_doitacvanchuyen"] = Opp["customerid"];
                        }
                        else
                        {
                            Hd["new_doitacvanchuyenkhdn"] = Opp["customerid"];
                        }
                        Hdid = service.Create(Hd);
                    }
                    else if (a == "100000006")
                    { //Hợp đồng cung ứng dịch vụ
                        Hd.LogicalName = "new_hopdongcungungdichvu";
                        if (((EntityReference)Opp["customerid"]).LogicalName == "contact")
                        {
                            Hd["new_doitaccungcap"] = Opp["customerid"];
                        }
                        else
                        {
                            Hd["new_doitaccungcapkhdn"] = Opp["customerid"];
                        }
                        Hd["new_loaicungcap"] = Opp["new_loaicungcap"];

                        Hdid = service.Create(Hd);
                    }
                    else if (a == "100000007")
                    { //Hợp đồng mua mía ngoài
                        Hd.LogicalName = "new_hopdongmuabanmiangoai";
                        if (((EntityReference)Opp["customerid"]).LogicalName == "contact")
                        {
                            Hd["new_khachhang"] = Opp["customerid"];
                        }
                        else
                        {
                            Hd["new_khachhangdoanhnghiep"] = Opp["customerid"];
                        }
                        Hdid = service.Create(Hd);
                    }
                }

                Entity oppClose = new Entity("opportunityclose");
                oppClose["opportunityid"] = new EntityReference("opportunity", Opp.Id);
                oppClose["actualend"] = DateTime.Now;
                oppClose["actualrevenue"] = new Money(Convert.ToDecimal("0"));

                OptionSetValue status = new OptionSetValue();
                status.Value = 3;

                WinOpportunityRequest winOppReq = new WinOpportunityRequest();
                winOppReq.OpportunityClose = oppClose;
                winOppReq.Status = new OptionSetValue(3);

                WinOpportunityResponse winOppResp = (WinOpportunityResponse)service.Execute(winOppReq);

                //}
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
