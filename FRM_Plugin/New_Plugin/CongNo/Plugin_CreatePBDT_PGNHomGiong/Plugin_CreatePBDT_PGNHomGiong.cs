using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk.Messages;

namespace Plugin_CreatePBDT_PGNHomGiong
{
    public class Plugin_CreatePBDT_PGNHomGiong : IPlugin
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
                Entity fullEntity = (Entity)context.PostEntityImages["PostImg"];
                Entity Vudautu = service.Retrieve("new_vudautu", ((EntityReference)fullEntity["new_vudautu"]).Id, new ColumnSet(new string[] { "new_mavudautu" }));
                Entity HDMia = service.Retrieve("new_hopdongdautumia", ((EntityReference)fullEntity["new_hopdongdautumia"]).Id, new ColumnSet(new string[] { "new_masohopdong" }));
                Entity KH = null;
                if (fullEntity.Contains("new_khachhang"))
                    KH = service.Retrieve("contact", ((EntityReference)fullEntity["new_khachhang"]).Id, new ColumnSet(new string[] { "new_makhachhang", "new_socmnd" }));
                else
                    KH = service.Retrieve("account", ((EntityReference)fullEntity["new_khachhangdoanhnghiep"]).Id, new ColumnSet(new string[] { "new_makhachhang", "new_masothue" }));
                Entity NongTruong = null;
                if (fullEntity.Contains("new_nongtruong"))
                    NongTruong = service.Retrieve("account", ((EntityReference)fullEntity["new_nongtruong"]).Id , new ColumnSet(new string[] { "new_makhachhang", "new_masothue" }));

                if (fullEntity.Contains("new_loaigiaonhanhom"))
                {
                    QueryExpression q1 = new QueryExpression("new_chitietgiaonhanhomgiong");
                    q1.ColumnSet = new ColumnSet(true);
                    q1.Criteria.AddCondition(new ConditionExpression("new_phieugiaonhanhomgiong", ConditionOperator.Equal, target.Id));
                    q1.Criteria.AddCondition(new ConditionExpression("statecode", ConditionOperator.Equal, 0));
                    EntityCollection dsChiTietGN = service.RetrieveMultiple(q1);

                    if (((OptionSetValue)fullEntity["new_loaigiaonhanhom"]).Value == 100000000) //Nhà may - nong dan
                    {
                        //Ghi nợ hoàn lại.
                        if (fullEntity.Contains("new_tongsotienhl") && ((Money)fullEntity["new_tongsotienhl"]).Value > 0) {
                            //gen ETL transaction
                            #region begin

                            //Tạo Credit Nông dân
                            Entity etl_ND = new Entity("new_etltransaction");
                            etl_ND["new_name"] = fullEntity["new_masophieu"].ToString();
                            etl_ND["new_vouchernumber"] = "DTND";
                            etl_ND["new_transactiontype"] = new OptionSetValue(3);
                            etl_ND["new_customertype"] = new OptionSetValue(fullEntity.Contains("new_khachhang") ? 1 : 2);
                            etl_ND["new_season"] = Vudautu["new_mavudautu"].ToString();
                            etl_ND["new_vudattu"] = fullEntity["new_vudautu"];
                            etl_ND["new_sochungtu"] = fullEntity["new_masophieu"].ToString();
                            etl_ND["new_lannhan"] = "";
                            etl_ND["new_contractnumber"] = HDMia["new_masohopdong"].ToString();
                            etl_ND["new_tradingpartner"] = (KH.LogicalName.ToLower().Trim() == "contact" ? 
                                ((KH.Contains("new_makhachhang") ? KH["new_makhachhang"].ToString() : "") +"_"+ (KH.Contains("new_socmnd") ? KH["new_socmnd"].ToString() : ""))
                                : 
                                ((KH.Contains("new_makhachhang") ? KH["new_makhachhang"].ToString() : "") + "_" + (KH.Contains("new_masothue") ? KH["new_masothue"].ToString() : ""))
                                );
                            etl_ND["new_suppliernumber"] = KH["new_makhachhang"].ToString();
                            etl_ND["new_suppliersite"] = "TAY NINH";
                            etl_ND["new_invoicedate"] = DateTime.Now;
                            etl_ND["new_descriptionheader"] = "Ghi nợ nhận hom giống";
                            etl_ND["new_terms"] = "Tra Ngay";
                            etl_ND["new_taxtype"] = "";
                            etl_ND["new_invoiceamount"] = (Money)fullEntity["new_tongsotienhl"];

                            service.Create(etl_ND);

                            //Tạo tk 154 cho nông trường.

                            Entity etl_NT = new Entity("new_etltransaction");
                            etl_NT["new_name"] = fullEntity["new_masophieu"].ToString()+"_TK154";
                            etl_ND["new_vouchernumber"] = "DTND";
                            etl_NT["new_transactiontype"] = new OptionSetValue(3);
                            etl_NT["new_customertype"] = new OptionSetValue(7);
                            etl_NT["new_season"] = Vudautu["new_mavudautu"].ToString();
                            etl_NT["new_vudattu"] = fullEntity["new_vudautu"];
                            etl_NT["new_sochungtu"] = fullEntity["new_masophieu"].ToString();
                            etl_NT["new_lannhan"] = "";
                            etl_NT["new_contractnumber"] = HDMia["new_masohopdong"].ToString();
                            if (NongTruong != null)
                                etl_NT["new_tradingpartner"] = ((NongTruong.Contains("new_makhachhang") ? NongTruong["new_makhachhang"].ToString() : "") +"_"+ (NongTruong.Contains("new_masothue") ? NongTruong["new_masothue"].ToString() : ""));
                            else
                                etl_NT["new_tradingpartner"] = "";
                           
                            etl_NT["new_suppliernumber"] = KH["new_makhachhang"].ToString();
                            etl_NT["new_suppliersite"] = "TAY NINH";
                            etl_NT["new_invoicedate"] = DateTime.Now;
                            etl_NT["new_descriptionheader"] = "Hạch toán 154 hom nông trường giao nông dân";
                            etl_NT["new_terms"] = "Tra Ngay";
                            etl_NT["new_taxtype"] = "";
                            etl_NT["new_invoiceamount"] = (Money)fullEntity["new_tongsotienhl"];

                            service.Create(etl_NT);

                            #endregion
                        }

                        if (fullEntity.Contains("new_tongsotienkhl") && ((Money)fullEntity["new_tongsotienkhl"]).Value > 0)
                        {
                            #region begin
                            //tạo credit
                            Entity etl_ND = new Entity("new_etltransaction");
                            etl_ND["new_name"] = fullEntity["new_masophieu"].ToString() + "_KHL";
                            etl_ND["new_vouchernumber"] = "DTND";
                            etl_ND["new_transactiontype"] = new OptionSetValue(3);
                            etl_ND["new_customertype"] = new OptionSetValue(fullEntity.Contains("new_khachhang") ? 1 : 2);
                            etl_ND["new_season"] = Vudautu["new_mavudautu"].ToString();
                            etl_ND["new_vudattu"] = fullEntity["new_vudautu"];
                            etl_ND["new_sochungtu"] = fullEntity["new_masophieu"].ToString();
                            etl_ND["new_lannhan"] = "";
                            etl_ND["new_contractnumber"] = HDMia["new_masohopdong"].ToString();
                            etl_ND["new_tradingpartner"] = (KH.LogicalName.ToLower().Trim() == "contact" ?
                                ((KH.Contains("new_makhachhang") ? KH["new_makhachhang"].ToString() : "") + "_" + (KH.Contains("new_socmnd") ? KH["new_socmnd"].ToString() : ""))
                                :
                                ((KH.Contains("new_makhachhang") ? KH["new_makhachhang"].ToString() : "") + "_" + (KH.Contains("new_masothue") ? KH["new_masothue"].ToString() : ""))
                                );
                            etl_ND["new_suppliernumber"] = KH["new_makhachhang"].ToString();
                            etl_ND["new_suppliersite"] = "TAY NINH";
                            etl_ND["new_invoicedate"] = DateTime.Now;
                            etl_ND["new_descriptionheader"] = "Ghi nợ nhận hỗ trợ hom giống";
                            etl_ND["new_terms"] = "Tra Ngay";
                            etl_ND["new_taxtype"] = "";
                            etl_ND["new_invoiceamount"] = (Money)fullEntity["new_tongsotienkhl"];

                            service.Create(etl_ND);

                            //Tạo tk 154 cho nông trường.

                            Entity etl_NT = new Entity("new_etltransaction");
                            etl_NT["new_name"] = fullEntity["new_masophieu"].ToString() + "_KHL_TK154";
                            etl_ND["new_vouchernumber"] = "DTND";
                            etl_NT["new_transactiontype"] = new OptionSetValue(3);
                            etl_NT["new_customertype"] = new OptionSetValue(7);
                            etl_NT["new_season"] = Vudautu["new_mavudautu"].ToString();
                            etl_NT["new_vudattu"] = fullEntity["new_vudautu"];
                            etl_NT["new_sochungtu"] = fullEntity["new_masophieu"].ToString();
                            etl_NT["new_lannhan"] = "";
                            etl_NT["new_contractnumber"] = HDMia["new_masohopdong"].ToString();
                            if (NongTruong != null)
                                etl_NT["new_tradingpartner"] = ((NongTruong.Contains("new_makhachhang") ? NongTruong["new_makhachhang"].ToString() : "") + "_" + (NongTruong.Contains("new_masothue") ? NongTruong["new_masothue"].ToString() : ""));
                            else
                                etl_NT["new_tradingpartner"] = "";

                            etl_NT["new_suppliernumber"] = KH["new_makhachhang"].ToString();
                            etl_NT["new_suppliersite"] = "TAY NINH";
                            etl_NT["new_invoicedate"] = DateTime.Now;
                            etl_NT["new_descriptionheader"] = "Hạch toán 154 hom nông trường giao nông dân";
                            etl_NT["new_terms"] = "Tra Ngay";
                            etl_NT["new_taxtype"] = "";
                            etl_NT["new_invoiceamount"] = (Money)fullEntity["new_tongsotienhl"];

                            service.Create(etl_NT);

                            // STA
                            Entity etl_STA = new Entity("new_etltransaction");
                            etl_STA["new_name"] = fullEntity["new_masophieu"].ToString() + "_STA";
                            etl_STA["new_vouchernumber"] = "DTND";
                            etl_STA["new_transactiontype"] = new OptionSetValue(3);
                            etl_STA["new_customertype"] = new OptionSetValue(fullEntity.Contains("new_khachhang") ? 1 : 2);
                            etl_STA["new_season"] = Vudautu["new_mavudautu"].ToString();
                            etl_STA["new_vudattu"] = fullEntity["new_vudautu"];
                            etl_STA["new_sochungtu"] = fullEntity["new_masophieu"].ToString();
                            etl_STA["new_lannhan"] = "";
                            etl_STA["new_contractnumber"] = HDMia["new_masohopdong"].ToString();
                            etl_STA["new_tradingpartner"] = (KH.LogicalName.ToLower().Trim() == "contact" ?
                                ((KH.Contains("new_makhachhang") ? KH["new_makhachhang"].ToString() : "") + "_" + (KH.Contains("new_socmnd") ? KH["new_socmnd"].ToString() : ""))
                                :
                                ((KH.Contains("new_makhachhang") ? KH["new_makhachhang"].ToString() : "") + "_" + (KH.Contains("new_masothue") ? KH["new_masothue"].ToString() : ""))
                                );
                            etl_STA["new_suppliernumber"] = KH["new_makhachhang"].ToString();
                            etl_STA["new_suppliersite"] = "TAY NINH";
                            etl_STA["new_invoicedate"] = DateTime.Now;
                            etl_STA["new_descriptionheader"] = "Ghi nợ nhận hỗ trợ hom giống";
                            etl_STA["new_terms"] = "Tra Ngay";
                            etl_STA["new_taxtype"] = "";
                            etl_STA["new_invoiceamount"] = (Money)fullEntity["new_tongsotienkhl"];

                            service.Create(etl_STA);

                            //Pay cấn trừ
                            Entity pay1 = new Entity("new_applytransaction");
                            pay1[""]


                            #endregion
                        }

                        //gen phân bổ đầu tư
                        #region begin
                        Entity pbdt = new Entity("new_phanbodautu");








                        #endregion 
                    }
                    else //Nong dan - nong dan
                    {
                        //gen phân bổ đầu tư
                        #region begin




                        #endregion 
                    }
                }
                else throw new InvalidPluginExecutionException("Phiếu giao nhận hom giống chưa chọn Loại giao nhận hom !");
            }
            

        }

    }
}
