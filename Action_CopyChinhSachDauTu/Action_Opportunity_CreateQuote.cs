using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.ServiceModel;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using Microsoft.Xrm.Sdk.Client;
using Microsoft.Xrm.Sdk.Discovery;
using Microsoft.Crm.Sdk.Messages;
using System.IO;
using System.Xml.Linq;
using System.Web;
using System.Runtime.Serialization;

namespace Action_CopyChinhSachDauTu
{
    public class Action_Opportunity_CreateQuote : IPlugin
    {
        IOrganizationService service = null;
        IOrganizationServiceFactory factory = null;
        void IPlugin.Execute(IServiceProvider serviceProvider)
        {
            IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            EntityReference target = (EntityReference)context.InputParameters["Target"];
            if (target.LogicalName == "opportunity")
            {
                factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
                service = factory.CreateOrganizationService(context.UserId);
                Entity oppEn = service.Retrieve(target.LogicalName, target.Id, new ColumnSet(true));
                if (oppEn == null)
                    throw new Exception("Cơ hội này không tồn tại!");

                int transactiontype = oppEn.Attributes.Contains("new_loaigiaodich") ? ((OptionSetValue)oppEn["new_loaigiaodich"]).Value : 0;
                if (transactiontype != 0)
                {
                    EntityCollection checkTran = GetTransactionProduct(oppEn.Id.ToString());
                    if (checkTran.Entities.Count <= 0)
                        throw (new Exception("Không có sản phẩm hợp lệ để báo giá. Vui lòng chọn sản phẩm để giao dịch!"));

                    bool Opp_typeofcustomer = (bool)oppEn["new_typeofcustomer"];

                    Entity contact = new Entity();
                    Entity account = new Entity();
                    Entity PriceList = new Entity();

                    Entity newQuote = new Entity("quote");

                    string Topic = oppEn.Attributes.Contains("name") ? oppEn["name"].ToString() : "";
                    newQuote["name"] = "New oppEn - " + Topic;

                    newQuote["bsd_typeofcustomer"] = Opp_typeofcustomer;
                    newQuote["bsd_loaigiaodich"] = new OptionSetValue(transactiontype);
                    newQuote["opportunityid"] = oppEn.ToEntityReference();

                    if (oppEn.Attributes.Contains("new_accounttransactionvfm"))
                    {
                        Entity AccountVFM = service.Retrieve("new_taikhoan", ((EntityReference)oppEn["new_accounttransactionvfm"]).Id, new ColumnSet("new_taikhoanid", "new_name"));
                        newQuote["bsd_accounttransactionvfm"] = AccountVFM.ToEntityReference();
                    }

                    if (oppEn.Attributes.Contains("new_promotions"))
                    {
                        Entity Promotions = service.Retrieve("new_promotion", ((EntityReference)oppEn["new_promotions"]).Id, new ColumnSet("new_promotionid", "new_name"));
                        newQuote["bsd_promotions"] = Promotions.ToEntityReference();
                    }

                    if (oppEn.Attributes.Contains("pricelevelid"))
                    {
                        PriceList = service.Retrieve("pricelevel", ((EntityReference)oppEn["pricelevelid"]).Id, new ColumnSet("pricelevelid", "name"));
                        newQuote["pricelevelid"] = PriceList.ToEntityReference();
                    }

                    bool bydistributionagents = (bool)oppEn["new_bydistributionagents"];
                    newQuote["bsd_bydistributionagents"] = bydistributionagents;

                    if (bydistributionagents == true)
                    {
                        if (oppEn.Attributes.Contains("bsd_distributor"))
                        {
                            Entity distributor = service.Retrieve("bsd_distributor", ((EntityReference)oppEn["bsd_distributor"]).Id, new ColumnSet("bsd_distributorid", "bsd_name"));
                            newQuote["bsd_distributor"] = distributor.ToEntityReference();
                        }

                        if (oppEn.Attributes.Contains("bsd_salesmanofdistributor"))
                        {
                            Entity salesmanofdistributor = service.Retrieve("bsd_saleofdistributor", ((EntityReference)oppEn["bsd_salesmanofdistributor"]).Id, new ColumnSet("bsd_saleofdistributoris", "bsd_name"));
                            newQuote["bsd_salesmanofdistributor"] = salesmanofdistributor.ToEntityReference();
                        }

                    }

                    if (Opp_typeofcustomer == false) // contact
                    {
                        if (oppEn.Attributes.Contains("parentcontactid"))
                            contact = service.Retrieve("contact", ((EntityReference)oppEn["parentcontactid"]).Id, new ColumnSet("contactid", "new_trangthaikhachhang", "fullname"));
                        newQuote["bsd_contact"] = contact.ToEntityReference();
                        newQuote["customerid"] = contact.ToEntityReference();
                    }
                    else
                    {
                        if (oppEn.Attributes.Contains("parentaccountid"))
                            account = service.Retrieve("account", ((EntityReference)oppEn["parentaccountid"]).Id, new ColumnSet("accountid", "name", "new_statusoftransaction"));
                        newQuote["bsd_account"] = account.ToEntityReference();
                        newQuote["customerid"] = account.ToEntityReference();
                    }

                    Guid newQuoteID = service.Create(newQuote);

                    EntityCollection TransPro = GetTransactionProduct(oppEn.Id.ToString());
                    if (TransPro.Entities.Count > 0)
                    {
                        foreach (Entity Trans in TransPro.Entities)
                        {
                            //Get Infor TranPro
                            string name = Trans.Attributes.Contains("bsd_name") ? Trans["bsd_name"].ToString() : "";
                            int times = Trans.Attributes.Contains("bsd_times") ? ((OptionSetValue)Trans["bsd_times"]).Value : 0;
                            decimal priceperunit = Trans.Attributes.Contains("bsd_priceperunit") ? ((Money)Trans["bsd_priceperunit"]).Value : 0;
                            decimal amount = Trans.Attributes.Contains("bsd_amountofpurchase") ? ((Money)Trans["bsd_amountofpurchase"]).Value : 0;
                            decimal quantity = Trans.Attributes.Contains("bsd_quantity") ? (Decimal)Trans["bsd_quantity"] : 0;
                            string ProductCode = Trans.Attributes.Contains("bsd_productcode") ? Trans["bsd_productcode"].ToString() : "";

                            //Phi phat hanh
                            decimal issuancefeeamount = Trans.Attributes.Contains("bsd_issuancefeeamount") ? ((Money)Trans["bsd_issuancefeeamount"]).Value : 0;
                            decimal phiphathanh = Trans.Attributes.Contains("bsd_phiphathanh") ? (Decimal)Trans["bsd_phiphathanh"] : 0;
                            // Phi mua lai
                            decimal repurchasefeeamount = Trans.Attributes.Contains("bsd_repurchasefeeamount") ? ((Money)Trans["bsd_repurchasefeeamount"]).Value : 0;
                            decimal phimualai = Trans.Attributes.Contains("bsd_phimualai") ? (Decimal)Trans["bsd_phimualai"] : 0;
                            // Phi chuyen doi
                            decimal conversionfeeamount = Trans.Attributes.Contains("bsd_conversionfeeamount") ? ((Money)Trans["bsd_conversionfeeamount"]).Value : 0;
                            decimal conversionfeepercent = Trans.Attributes.Contains("bsd_conversionfeepercent") ? (Decimal)Trans["bsd_conversionfeepercent"] : 0;
                            //tong phi
                            decimal sotienphi = Trans.Attributes.Contains("bsd_sotienphi") ? ((Money)Trans["bsd_sotienphi"]).Value : 0;
                            decimal phantramphi = Trans.Attributes.Contains("bsd_phantramphi") ? (Decimal)Trans["bsd_phantramphi"] : 0;
                            //discount
                            decimal discountamount = Trans.Attributes.Contains("bsd_discountamount") ? ((Money)Trans["bsd_discountamount"]).Value : 0;
                            decimal discountpercent = Trans.Attributes.Contains("bsd_discountpercent") ? (Decimal)Trans["bsd_discountpercent"] : 0;
                            //person incom tax
                            decimal personalincometaxpercent = Trans.Attributes.Contains("bsd_personalincometaxpercent") ? (decimal)Trans["bsd_personalincometaxpercent"] : 0;
                            decimal personalincometaxamount = Trans.Attributes.Contains("bsd_personalincometaxamount") ? ((Money)Trans["bsd_personalincometaxamount"]).Value : 0;

                            Entity TranQuote = new Entity("bsd_transactionproductquote");
                            TranQuote["bsd_name"] = name;
                            TranQuote["bsd_typeofcustomer"] = Opp_typeofcustomer;
                            TranQuote["bsd_typeoftransaction"] = new OptionSetValue(transactiontype);
                            TranQuote["bsd_times"] = new OptionSetValue(times);
                            TranQuote["bsd_priceperunit"] = new Money(priceperunit);
                            TranQuote["bsd_productcode"] = ProductCode;
                            TranQuote["bsd_pricelist"] = PriceList.ToEntityReference();


                            Entity QuoteEn = new Entity("quote");
                            QuoteEn.Id = newQuoteID;

                            TranQuote["bsd_quote"] = QuoteEn.ToEntityReference();

                            if (Trans.Attributes.Contains("bsd_ownership"))
                            {
                                Entity Ownership = service.Retrieve("new_sohuu", ((EntityReference)Trans["bsd_ownership"]).Id, new ColumnSet("new_sohuuid", "new_name"));
                                TranQuote["bsd_ownership"] = Ownership.ToEntityReference();
                            }

                            if (Trans.Attributes.Contains("bsd_product"))
                            {
                                Entity Product = service.Retrieve("product", ((EntityReference)Trans["bsd_product"]).Id, new ColumnSet("productnumber", "name"));
                                TranQuote["bsd_product"] = Product.ToEntityReference();
                            }

                            if (Trans.Attributes.Contains("bsd_product"))
                            {
                                Entity Unit = service.Retrieve("uom", ((EntityReference)Trans["bsd_unit"]).Id, new ColumnSet("uomid", "name"));
                                TranQuote["bsd_unit"] = Unit.ToEntityReference();
                            }


                            if (Opp_typeofcustomer == false) // contact
                                TranQuote["bsd_contact"] = contact.ToEntityReference();
                            else
                                TranQuote["bsd_account"] = account.ToEntityReference();

                            TranQuote["bsd_amount"] = new Money(amount);
                            TranQuote["bsd_quantity"] = quantity;
                            TranQuote["bsd_personalincometaxamount"] = new Money(personalincometaxamount);
                            TranQuote["bsd_personalincometaxpercent"] = personalincometaxpercent;

                            service.Create(TranQuote);
                        }
                    }

                    context.OutputParameters["ReturnId"] = newQuoteID.ToString();

                }
                else
                    throw (new Exception("Vui lòng chọn loại giao dịch trước khi báo giá!"));


            }
        }

        private EntityCollection GetTransactionProduct(string opId)
        {
            QueryExpression query = new QueryExpression("bsd_transactionproduct");
            int statuscode = 1; // Active

            query.ColumnSet = new ColumnSet(true);
            query.Distinct = true;
            query.Criteria = new FilterExpression();
            query.Criteria.AddCondition("bsd_opportunity", ConditionOperator.Equal, opId);
            query.Criteria.AddCondition("statuscode", ConditionOperator.Equal, statuscode);

            EntityCollection TransProList = service.RetrieveMultiple(query);
            return TransProList;
        }
    }
}
