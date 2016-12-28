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
            //OrganizationService service;
            //var connectionstring = GetWindowsIntegratedSecurityConnectionString();
            //var serverConnection = new ServerConnection(connectionstring);

            var credentials = new ClientCredentials();
            credentials.UserName.UserName = @"dev2\crmservices";
            credentials.UserName.Password = @"P@ssword";

            Uri OrganizationUri = new Uri("http://10.33.1.93/TEST/XRMServices/2011/Organization.svc");
            Uri HomeRealmUri = null;


            using (OrganizationServiceProxy serviceProxy = new OrganizationServiceProxy(OrganizationUri, HomeRealmUri, credentials, null))
            //using (service = new OrganizationService(serverConnection.CRMConnection))
            {
                IOrganizationService service = (IOrganizationService)serviceProxy;

                Guid entityId = new Guid("FDBE2E5E-1C3A-E611-93EF-98BE942A7E2D");
                Entity NTkhac = service.Retrieve("new_nghiemthukhac", entityId, new ColumnSet(new string[] { "new_hopdongdautumia", "new_chitiethddtmia", "new_khuyenkhichphattrien", "new_mohinhkhuyennong" }));

                if (NTkhac.Contains("new_hopdongdautumia") && NTkhac.Contains("new_chitiethddtmia"))
                {
                    EntityReference ChiTietHDRef = NTkhac.GetAttributeValue<EntityReference>("new_chitiethddtmia");
                    Guid ChiTietHDId = ChiTietHDRef.Id;
                    Entity ChiTietHD = service.Retrieve("new_thuadatcanhtac", ChiTietHDId, new ColumnSet(new string[] { "new_thamgiamohinhkhuyennong" }));

                    decimal tienbsHL = 0;
                    decimal tienbsKHL = 0;
                    decimal tienbsPB = 0;
                    decimal tongtienbs = 0;

                    Entity en = new Entity(NTkhac.LogicalName);
                    en.Id = NTkhac.Id;

                    // Tim CSBS khi có Mo hinh khuyen nong
                    if (NTkhac.Contains("new_mohinhkhuyennong") && !NTkhac.Contains("new_khuyenkhichphattrien"))
                    {
                        if (ChiTietHD.Contains("new_thamgiamohinhkhuyennong"))
                        {
                            EntityReference MHKNRef = NTkhac.GetAttributeValue<EntityReference>("new_mohinhkhuyennong");
                            Entity MHKN = service.Retrieve("new_mohinhkhuyennong", MHKNRef.Id, new ColumnSet(new string[] { "new_name" }));

                            EntityCollection dsCSDTBSbyMHKN = FindCSDTBSbyMHKN(service, MHKN);

                            EntityReference MHKNtrongChitietHDRef = ChiTietHD.GetAttributeValue<EntityReference>("new_thamgiamohinhkhuyennong");
                            //Entity MHKNtrongChitietHD = service.Retrieve("new_mohinhkhuyennong", MHKNtrongChitietHDRef.Id, new ColumnSet(new string[] { "new_name" }));

                            if (MHKNRef.Id == MHKNtrongChitietHDRef.Id)
                            {
                                if (dsCSDTBSbyMHKN != null && dsCSDTBSbyMHKN.Entities.Count > 0)
                                {
                                    Entity a = dsCSDTBSbyMHKN[0];

                                    tienbsHL = (a.Contains("new_sotienbosung") ? a.GetAttributeValue<Money>("new_sotienbosung").Value : 0);
                                    tienbsKHL = (a.Contains("new_sotienbosung_khl") ? a.GetAttributeValue<Money>("new_sotienbosung_khl").Value : 0);
                                    tienbsPB = (a.Contains("new_bosungphanbon") ? a.GetAttributeValue<Money>("new_bosungphanbon").Value : 0);

                                    tongtienbs = tienbsHL + tienbsKHL + tienbsPB;
                                    en["new_thanhtien"] = new Money(tongtienbs);
                                    en["new_chinhsachdautubosung"] = a.ToEntityReference();
                                    service.Update(en);
                                }
                            }
                            else
                                throw new InvalidPluginExecutionException("Thửa đất không tham gia mô hình khuyến nông " + MHKN["new_name"].ToString());
                        }

                    } // if (NTkhac.Contains("new_mohinhkhuyennong") && !NTkhac.Contains("new_khuyenkhichphattrien"))

                    // Tim CSBS khi có Khuyen khich phat trien
                    if (NTkhac.Contains("new_khuyenkhichphattrien") && !NTkhac.Contains("new_mohinhkhuyennong"))
                    {
                        EntityCollection dsKKPTHDCT = RetrieveNNRecord(service, "new_khuyenkhichphattrien", "new_thuadatcanhtac", "new_new_chitiethddtmia_new_khuyenkhichpt", new ColumnSet(new string[] { "new_khuyenkhichphattrienid" }), "new_thuadatcanhtacid", ChiTietHD.Id);

                        EntityReference KKPTcuaNTkhacRef = NTkhac.GetAttributeValue<EntityReference>("new_khuyenkhichphattrien");
                        Entity KKPTcuaNTkhac = service.Retrieve("new_khuyenkhichphattrien", KKPTcuaNTkhacRef.Id, new ColumnSet(new string[] { "new_name" }));

                        if (dsKKPTHDCT != null && dsKKPTHDCT.Entities.Count > 0)
                        {
                            foreach (Entity KKPTHDCT in dsKKPTHDCT.Entities)
                            {
                                if (KKPTHDCT.Id == KKPTcuaNTkhac.Id)
                                {
                                    EntityCollection dsCSDTBSbyKKPT = FindCSDTBSbyKKPT(service, KKPTcuaNTkhac);
                                    if (dsCSDTBSbyKKPT != null && dsCSDTBSbyKKPT.Entities.Count > 0)
                                    {
                                        Entity a = dsCSDTBSbyKKPT[0];
                                        tienbsHL = (a.Contains("new_sotienbosung") ? a.GetAttributeValue<Money>("new_sotienbosung").Value : 0);
                                        tienbsKHL = (a.Contains("new_sotienbosung_khl") ? a.GetAttributeValue<Money>("new_sotienbosung_khl").Value : 0);
                                        tienbsPB = (a.Contains("new_bosungphanbon") ? a.GetAttributeValue<Money>("new_bosungphanbon").Value : 0);

                                        tongtienbs = tienbsHL + tienbsKHL + tienbsPB;
                                        en["new_thanhtien"] = new Money(tongtienbs);
                                        en["new_chinhsachdautubosung"] = a.ToEntityReference();
                                        service.Update(en);
                                    }
                                }
                                else
                                    throw new InvalidPluginExecutionException("Thửa đất không tham gia khuyến khích phát triển này");
                            }
                        }
                        else
                            throw new InvalidPluginExecutionException("Thửa đất không tham gia khuyến khích phát triển ");

                    } // if (NTkhac.Contains("new_khuyenkhichphattrien") && !NTkhac.Contains("new_mohinhkhuyennong"))
                }
            }//using
        }

        public static EntityCollection RetrieveNNRecord(IOrganizationService crmservices, string entity1, string entity2, string relateName, ColumnSet column, string condition, object value)
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
            EntityCollection collRecords = crmservices.RetrieveMultiple(query);

            return collRecords;
        }

        public static EntityCollection FindCSDTBSbyMHKN(IOrganizationService crmservices, Entity MHNK)
        {
            string fetchXml =
                   @"<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='false'>
                      <entity name='new_chinhsachdautuchitiet'>
                        <attribute name='new_chinhsachdautuchitietid' />
                        <attribute name='new_name' />
                        <attribute name='createdon' />
                        <attribute name='new_sotienbosung' />
                        <attribute name='new_sotienbosung_khl' />
                        <attribute name='new_bosungphanbon' />
                        <order attribute='new_name' descending='false' />
                        <filter type='and'>
                          <condition attribute='new_mohinhkhuyennong' operator='eq' uitype='new_mohinhkhuyennong' value='{0}' />
                          <condition attribute='new_nghiemthu' operator='eq' value='1'/>
                        </filter>
                      </entity>
                    </fetch>";

            fetchXml = string.Format(fetchXml, MHNK.Id);
            EntityCollection entc = crmservices.RetrieveMultiple(new FetchExpression(fetchXml));

            return entc;
        }

        public static EntityCollection FindCSDTBSbyKKPT(IOrganizationService crmservices, Entity KKPT)
        {
            string fetchXml =
                   @"<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='false'>
                      <entity name='new_chinhsachdautuchitiet'>
                        <attribute name='new_chinhsachdautuchitietid' />
                        <attribute name='new_name' />
                        <attribute name='createdon' />
                        <attribute name='new_sotienbosung' />
                        <attribute name='new_sotienbosung_khl' />
                        <attribute name='new_bosungphanbon' />
                        <order attribute='new_name' descending='false' />
                        <filter type='and'>
                          <condition attribute='new_khuyenkhichphattrien' operator='eq' uitype='new_khuyenkhichphattrien' value='{0}' />
                          <condition attribute='new_nghiemthu' operator='eq' value='1'/>
                        </filter>
                      </entity>
                    </fetch>";

            fetchXml = string.Format(fetchXml, KKPT.Id);
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