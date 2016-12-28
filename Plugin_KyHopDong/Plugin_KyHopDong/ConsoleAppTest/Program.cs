using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Client;
using Microsoft.Xrm.Sdk.Query;
using Microsoft.Xrm.Sdk.Discovery;
using Microsoft.Xrm.Sdk.XmlNamespaces;
using Microsoft.Xrm.Client.Services;
using System.Configuration;
using Microsoft.Xrm.Sdk.Messages;
using Microsoft.Xrm.Sdk.Metadata;
using Microsoft.Xrm.Sdk.WebServiceClient;
using System.ServiceModel;
using System.ServiceModel.Security;
using System.ServiceModel.Description;

namespace ConsoleAppTest
{
    class Program
    {
        static void Main(string[] args)
        {
            var credentials = new ClientCredentials();
            credentials.UserName.UserName = @"dev2\crmservices";
            credentials.UserName.Password = @"P@ssword";

            Uri OrganizationUri = new Uri("http://10.33.1.93/TEST/XRMServices/2011/Organization.svc");
            Uri HomeRealmUri = null;

            using (OrganizationServiceProxy serviceProxy = new OrganizationServiceProxy(OrganizationUri, HomeRealmUri, credentials, null))
            {
                IOrganizationService service = (IOrganizationService)serviceProxy;

                Guid entityId = new Guid("F333C7A5-8038-E611-93EF-98BE942A7E2D");
                Entity HD = service.Retrieve("new_phieuthamdinhdautu", entityId, new ColumnSet(true));

                if (HD.LogicalName.Trim().ToLower() == "new_phieuthamdinhdautu")
                {
                    if (HD.Contains("new_tinhtrangduyet"))
                        if (((OptionSetValue)HD["new_tinhtrangduyet"]).Value == 100000006)
                        {
                            Entity PTD = new Entity("new_phieuthamdinhdautu");
                            PTD.Id = HD.Id;
                            PTD["statuscode"] = new OptionSetValue(100000000);
                            service.Update(PTD);

                            // Cập nhật Trạng thái đề nghị đầu tư = Hoàn tất
                            if (HD.Contains("new_denghidautu"))
                            {
                                Entity uHD = new Entity("opportunity");
                                uHD.Id = ((EntityReference)HD["new_denghidautu"]).Id;
                                uHD["statuscode"] = new OptionSetValue(100000006);
                                service.Update(uHD);
                            }

                            if (HD.Contains("new_hopdongdautumia"))
                            {
                                Entity uHD = new Entity("new_hopdongdautumia");
                                uHD.Id = ((EntityReference)HD["new_hopdongdautumia"]).Id;

                                uHD = service.Retrieve("new_hopdongdautumia", ((EntityReference)HD["new_hopdongdautumia"]).Id, new ColumnSet(new string[] { "statuscode" }));

                                if (uHD.Contains("statuscode") && (((OptionSetValue)uHD["statuscode"]).Value.ToString() == "1" || ((OptionSetValue)uHD["statuscode"]).Value.ToString() == "100000005"))
                                {
                                    uHD["statuscode"] = new OptionSetValue(100000003);
                                    uHD["new_ngaykyhopdong"] = HD["new_ngayky"];
                                }
                                service.Update(uHD);

                                Entity HDDTmia = service.Retrieve("new_hopdongdautumia", ((EntityReference)HD["new_hopdongdautumia"]).Id, new ColumnSet(new string[] { "new_name" }));

                                EntityCollection dsCTHDDT = FindCTHDDTmia(service, HDDTmia);
                                if (dsCTHDDT != null && dsCTHDDT.Entities.Count > 0)
                                {
                                    foreach (Entity a in dsCTHDDT.Entities)
                                    {
                                        if (a.Contains("statuscode") && ((OptionSetValue)a["statuscode"]).Value.ToString() == "1")
                                        {
                                            if (a.Contains("new_dongiahopdong") || a.Contains("new_dongiahopdongkhl") || a.Contains("new_dongiaphanbonhd"))
                                            {
                                                Entity en = new Entity(a.LogicalName);
                                                en.Id = a.Id;

                                                en["statuscode"] = new OptionSetValue(100000000);
                                                en["new_trangthainghiemthu"] = new OptionSetValue(100000001);
                                                service.Update(en);
                                            }
                                        }
                                    }
                                }
                            }
                            if (HD.Contains("new_hopdongdaututhuedat"))
                            {
                                Entity uHD = new Entity("new_hopdongthuedat");
                                uHD.Id = ((EntityReference)HD["new_hopdongdaututhuedat"]).Id;

                                uHD = service.Retrieve("new_hopdongthuedat", ((EntityReference)HD["new_hopdongdaututhuedat"]).Id, new ColumnSet(new string[] { "statuscode" }));

                                if (uHD.Contains("statuscode") && (((OptionSetValue)uHD["statuscode"]).Value.ToString() == "1" || ((OptionSetValue)uHD["statuscode"]).Value.ToString() == "100000005"))
                                {
                                    uHD["statuscode"] = new OptionSetValue(100000000);
                                    uHD["new_ngaykyhopdong"] = HD["new_ngayky"];
                                }
                                service.Update(uHD);

                                Entity HDDTthuedat = service.Retrieve("new_hopdongthuedat", ((EntityReference)HD["new_hopdongdaututhuedat"]).Id, new ColumnSet(new string[] { "new_name" }));

                                EntityCollection dsCTHDDT = FindCTHDDTthuedat(service, HDDTthuedat);
                                if (dsCTHDDT != null && dsCTHDDT.Entities.Count > 0)
                                {
                                    foreach (Entity a in dsCTHDDT.Entities)
                                    {
                                        if (a.Contains("new_trangthainghiemthu") && ((OptionSetValue)a["new_trangthainghiemthu"]).Value.ToString() == "100000000")
                                        {
                                            if (a.Contains("new_sotiendautu"))
                                            {
                                                Entity en = new Entity(a.LogicalName);
                                                en.Id = a.Id;

                                                en["new_trangthainghiemthu"] = new OptionSetValue(100000001);
                                                service.Update(en);
                                            }
                                        }
                                    }
                                }
                            }

                            if (HD.Contains("new_hopdongthechap"))
                            {
                                Entity uHD = new Entity("new_hopdongthechap");
                                uHD.Id = ((EntityReference)HD["new_hopdongthechap"]).Id;
                                uHD = service.Retrieve("new_hopdongthechap", ((EntityReference)HD["new_hopdongthechap"]).Id, new ColumnSet(new string[] { "statuscode" }));

                                if (uHD.Contains("statuscode") && ((OptionSetValue)uHD["statuscode"]).Value.ToString() == "1")
                                {
                                    uHD["statuscode"] = new OptionSetValue(100000000);
                                    uHD["new_tinhtrangduyet"] = new OptionSetValue(100000006);
                                }
                                service.Update(uHD);

                                Entity HDthechap = service.Retrieve("new_hopdongthechap", ((EntityReference)HD["new_hopdongthechap"]).Id, new ColumnSet(new string[] { "new_name" }));

                                EntityCollection dsTSTC = FindTSTC(service, HDthechap);
                                if (dsTSTC != null && dsTSTC.Entities.Count > 0)
                                {
                                    foreach (Entity a in dsTSTC.Entities)
                                    {
                                        if (a.Contains("new_giatrisosachgiatriquydinh") || a.Contains("new_giatridinhgiagiatrithechap"))
                                        {
                                            Entity en = new Entity(a.LogicalName);
                                            en.Id = a.Id;

                                            en["statuscode"] = new OptionSetValue(100000000);
                                            service.Update(en);
                                        }
                                    }
                                }
                            }

                            if (HD.Contains("new_khachhang"))
                            {
                                Entity KH = new Entity("contact");
                                KH.Id = ((EntityReference)HD["new_khachhang"]).Id;
                                KH["statuscode"] = new OptionSetValue(100000000);
                                service.Update(KH);
                            }
                            else
                            {
                                Entity KH = new Entity("account");
                                KH.Id = ((EntityReference)HD["new_khachhangdoanhnghiep"]).Id;
                                KH["statuscode"] = new OptionSetValue(100000000);
                                service.Update(KH);
                            }
                        }
                }
            }//using
        }

        public static EntityCollection FindCTHDDTmia(IOrganizationService crmservices, Entity HD)
        {
            string fetchXml =
                   @"<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='false'>
                      <entity name='new_thuadatcanhtac'>
                        <attribute name='new_name' />
                        <attribute name='statuscode' />
                        <attribute name='new_dongiahopdong' />
                        <attribute name='new_dongiahopdongkhl' />
                        <attribute name='new_dongiaphanbonhd' />
                        <attribute name='new_thuadatcanhtacid' />
                        <order attribute='new_name' descending='false' />
                        <filter type='and'>
                          <condition attribute='new_hopdongdautumia' operator='eq' uitype='new_hopdongdautumia' value='{0}' />
                        </filter>
                      </entity>
                    </fetch>";

            fetchXml = string.Format(fetchXml, HD.Id);
            EntityCollection entc = crmservices.RetrieveMultiple(new FetchExpression(fetchXml));

            return entc;
        }

        public static EntityCollection FindCTHDDTthuedat(IOrganizationService crmservices, Entity HD)
        {
            string fetchXml =
                   @"<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='false'>
                      <entity name='new_datthue'>
                        <attribute name='new_name' />
                        <attribute name='new_sotiendautu' />
                        <attribute name='new_trangthainghiemthu' />
                        <attribute name='new_datthueid' />
                        <order attribute='new_name' descending='false' />
                        <filter type='and'>
                          <condition attribute='new_hopdongthuedat' operator='eq' uitype='new_hopdongthuedat' value='{0}' />
                        </filter>
                      </entity>
                    </fetch>";

            fetchXml = string.Format(fetchXml, HD.Id);
            EntityCollection entc = crmservices.RetrieveMultiple(new FetchExpression(fetchXml));

            return entc;
        }

        public static EntityCollection FindTSTC(IOrganizationService crmservices, Entity HD)
        {
            string fetchXml =
                   @"<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='false'>
                      <entity name='new_taisanthechap'>
                        <attribute name='new_taisanthechapid' />
                        <attribute name='new_name' />
                        <attribute name='statuscode' />
                        <attribute name='new_giatrisosachgiatriquydinh' />
                        <attribute name='new_giatridinhgiagiatrithechap' />
                        <order attribute='new_name' descending='false' />
                        <filter type='and'>
                          <condition attribute='new_hopdongthechap' operator='eq' uitype='new_hopdongthechap' value='{0}' />
                        </filter>
                      </entity>
                    </fetch>";

            fetchXml = string.Format(fetchXml, HD.Id);
            EntityCollection entc = crmservices.RetrieveMultiple(new FetchExpression(fetchXml));

            return entc;
        }

    }
}


//StringBuilder xml = new StringBuilder();
// xml.AppendLine("<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='false'>");
// xml.AppendLine()
//stringA.AppendLine(stringB) nghia la stringA = stringA + stringB

// muốn check field có value không thì dùng service.Trace(entityA.Attributes.Contains("fieldxyz")); 
// sau đó dùng hàm Contains() để check coi nó có value ko
// hoặc dùng if (bien == null) Trace("A") else Trace("B");

//Logger.Write("Phonecall PostCreate", "Begin");
//throw new InvalidPluginExecutionException("End");
//Logger.Write("entity Id", entity.Id.ToString());

//Convert.ChangeType(mCSTM["new_dongiatang1ccs"]), decimal);
//giá trị mới = Convert.ChangeType(val, pd.PropertyType);
//service.Trace(entityA.Attributes.Contains("fieldxyz"));
//service.Trace("vi tri 1");