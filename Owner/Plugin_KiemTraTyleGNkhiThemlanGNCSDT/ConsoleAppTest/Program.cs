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

namespace ConsoleAppTest
{
    class Program
    {
        static void Main(string[] args)
        {
            OrganizationService service;
            var connectionstring = GetWindowsIntegratedSecurityConnectionString();
            var serverConnection = new ServerConnection(connectionstring);

            using (service = new OrganizationService(serverConnection.CRMConnection))
            {
                Guid entityId = new Guid("25569F14-5DC6-E511-93F1-9ABE942A7E29"); //  7b25569F14-5DC6-E511-93F1-9ABE942A7E29
                Entity Langiaingan = service.Retrieve("new_dinhmucdautu", entityId, new ColumnSet(true));

                if (Langiaingan.Attributes.Contains("new_chinhsachdautu"))
                {
                    EntityReference csdtEntityRef = Langiaingan.GetAttributeValue<EntityReference>("new_chinhsachdautu");
                    Guid csdtId = csdtEntityRef.Id;
                    Entity csdtObj = service.Retrieve("new_chinhsachdautu", csdtId, new ColumnSet(new string[] { "new_name" }));

                    decimal phantramdaGN = 0;
                    decimal phantramconlai = 0;
                    decimal phantramlannay = (Langiaingan.Contains("new_phantramtilegiaingan") ? (decimal)Langiaingan["new_phantramtilegiaingan"] : 0);

                    string fetchXml =
                              @"<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='false'>
                                      <entity name='new_dinhmucdautu'>
                                        <attribute name='new_name' />
                                        <attribute name='new_sotien' />
                                        <attribute name='new_langiaingan' />
                                        <attribute name='new_phantramtilegiaingan' />
                                        <attribute name='new_yeucauconghiemthu' />
                                        <attribute name='createdon' />
                                        <attribute name='new_dinhmucdautuid' />
                                        <order attribute='createdon' descending='true' />
                                        <filter type='and'>
                                          <condition attribute='new_chinhsachdautu' operator='eq' uitype='new_chinhsachdautu' value='{0}' />
                                        </filter>
                                      </entity>
                                    </fetch>";
                    fetchXml = string.Format(fetchXml, csdtId);
                    EntityCollection result = service.RetrieveMultiple(new FetchExpression(fetchXml));
                    foreach (Entity a in result.Entities)
                    {
                        phantramdaGN += (a.Contains("new_phantramtilegiaingan") ? (decimal)a["new_phantramtilegiaingan"] : 0);
                    }
                    phantramconlai = 100 - phantramdaGN;

                    if (phantramconlai <= 0)
                    {
                        throw new InvalidPluginExecutionException(" Tổng tỷ lệ phần trăm giải ngân đã đạt 100%");
                    }
                    if (phantramlannay > phantramconlai)
                    {
                        throw new InvalidPluginExecutionException("Phần trăm giải ngân tối đa còn lại là " + phantramconlai);
                    }
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

        public static string GetWindowsIntegratedSecurityConnectionString()
        {
            int count = ConfigurationManager.ConnectionStrings.Count;
            if (count == 0)
                throw new Exception("Could not find ConnectionString");

            return ConfigurationManager.ConnectionStrings[0].ConnectionString;
        }

        public static EntityCollection FindChinhSachDT(IOrganizationService crmservices, Entity ctHDDT)
        {
            Entity HD = crmservices.Retrieve("new_hopdongdautumia", ((EntityReference)ctHDDT["new_hopdongdautumia"]).Id, new ColumnSet(new string[] { "new_vudautu" }));
            Entity VDT = crmservices.Retrieve("new_vudautu", ((EntityReference)HD["new_vudautu"]).Id, new ColumnSet(new string[] { "new_name" }));

            QueryExpression q = new QueryExpression("new_chinhsachdautu");
            q.ColumnSet = new ColumnSet(true);
            //OptionSetValue[] values = new OptionSetValue[] { new OptionSetValue(100000000), new OptionSetValue(100000003) };
            q.Criteria = new FilterExpression();
            q.Criteria.AddCondition(new ConditionExpression("new_ngayapdung", ConditionOperator.LessEqual, ctHDDT["createdon"]));
            q.Criteria.AddCondition(new ConditionExpression("new_vudautu", ConditionOperator.Equal, VDT.Id));
            q.Criteria.AddCondition(new ConditionExpression("new_loaihopdong", ConditionOperator.Equal, 100000000));
            q.Criteria.AddCondition(new ConditionExpression("new_mucdichdautu", ConditionOperator.Equal, 100000000));
            q.Orders.Add(new OrderExpression("new_ngayapdung", OrderType.Descending));

            EntityCollection entc = crmservices.RetrieveMultiple(q);
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