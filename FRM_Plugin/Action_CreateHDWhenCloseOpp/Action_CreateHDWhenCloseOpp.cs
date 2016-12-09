using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk.Messages;
using Microsoft.Crm.Sdk.Messages;

namespace Action_CreateHDWhenCloseOpp
{
    public class Action_CreateHDWhenCloseOpp : IPlugin
    {
        IOrganizationService service = null;
        IOrganizationServiceFactory factory = null;
        void IPlugin.Execute(IServiceProvider serviceProvider)
        {

            IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            EntityReference target = (EntityReference)context.InputParameters["Target"];
            factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
            service = factory.CreateOrganizationService(context.UserId);
            string vl = "";
            try
            {
             
                Entity Opp = service.Retrieve(target.LogicalName, target.Id, new ColumnSet(true));
                if (((OptionSetValue)Opp["statuscode"]).Value != 3)
                {
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
                            context.OutputParameters["ReturnId"] = "success";
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
                            context.OutputParameters["ReturnId"] = "success";
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
                            Hdid = service.Create(Hd);
                            context.OutputParameters["ReturnId"] = "success";
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
                            context.OutputParameters["ReturnId"] = "success";
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
                            context.OutputParameters["ReturnId"] = "success";
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
                            context.OutputParameters["ReturnId"] = "success";
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
                            context.OutputParameters["ReturnId"] = "success";
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
                            context.OutputParameters["ReturnId"] = "success";
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
                    winOppReq.Status = status;

                    WinOpportunityResponse winOppResp = (WinOpportunityResponse)service.Execute(winOppReq);
                }
                else
                {
                    context.OutputParameters["ReturnId"] = "Cơ hội đã bị khóa, vui lòng duyệt cơ hội khác!";
                }
            }
            catch (Exception ex)
            {
                context.OutputParameters["ReturnId"] = ex.Message + "-" + vl;
            }
        }
    }
}
