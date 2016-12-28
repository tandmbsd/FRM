using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk.Messages;


namespace CheckThuaDatInHDDTMia
{
    public class CheckThuaDatInHDDTMia : IPlugin
    {
        // moi nhat 
        IOrganizationService service = null;
        IOrganizationServiceFactory factory = null;
        void IPlugin.Execute(IServiceProvider serviceProvider)
        {
            IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
            service = factory.CreateOrganizationService(context.UserId);
            Entity target = (Entity)context.InputParameters["Target"];

            if (context.MessageName.ToLower().Trim() == "create" && target.Contains("new_hopdongdautumia") && target.Contains("new_thuadat"))
            {
                int count = 0;
                int count1 = 0;

                Entity thuadatcanhtac = service.Retrieve(target.LogicalName, target.Id, new ColumnSet(new string[] { "new_hopdongdautumia", "new_thuadat", "statuscode" }));
                string statuscode = ((OptionSetValue)thuadatcanhtac["statuscode"]).Value.ToString();

                Entity thuadat = service.Retrieve("new_thuadat", ((EntityReference)thuadatcanhtac["new_thuadat"]).Id,
                    new ColumnSet(new string[] { "new_diachi" }));

                Entity hopdongdautumia = service.Retrieve("new_hopdongdautumia",
                    ((EntityReference)thuadatcanhtac["new_hopdongdautumia"]).Id,
                    new ColumnSet(new string[] { "new_vudautu", "statuscode", "new_quocgia" }));

                if ((hopdongdautumia.Contains("statuscode") && ((OptionSetValue)hopdongdautumia["statuscode"]).Value == 100000003) && (!target.Contains("new_phuluchopdongid")))                
                    throw new Exception("Hợp đồng đã ký không được phép thêm chi tiết hợp đồng đầu tư mía !!!");                

                //List<Entity> lstThuadatcanhtac1 = RetrieveMultiRecord(service, "new_thuadatcanhtac",
                //    new ColumnSet(true), "new_hopdongdautumia", hopdongdautumia.Id);

                QueryExpression q1 = new QueryExpression("new_thuadatcanhtac");
                q1.ColumnSet = new ColumnSet(new string[] { "new_hopdongdautumia", "new_thuadat", "new_name" });
                q1.Criteria = new FilterExpression(LogicalOperator.And);
                q1.Criteria.AddCondition(new ConditionExpression("new_hopdongdautumia", ConditionOperator.Equal, hopdongdautumia.Id)); 
                q1.Criteria.AddCondition(new ConditionExpression("statuscode", ConditionOperator.NotEqual, 100000007)); // ko bang thanh ly
                q1.Criteria.AddCondition(new ConditionExpression("statuscode", ConditionOperator.Equal, 100000000)); // bang da ky
                q1.Criteria.AddCondition(new ConditionExpression("statuscode", ConditionOperator.NotEqual, 100000005)); // ko bang huy
                EntityCollection entc1 = service.RetrieveMultiple(q1);
                List<Entity> lstThuadatcanhtac1 = entc1.Entities.ToList();

                foreach (Entity tdct in lstThuadatcanhtac1)
                {
                    if (thuadat.Id == ((EntityReference)tdct["new_thuadat"]).Id)                    
                        count++;                    
                }   

                if (count > 1)                
                    throw new Exception("Thửa đất đã tồn tại trong chi tiết khác của hợp đồng này !!!");                

                QueryExpression q = new QueryExpression("new_thuadatcanhtac");
                q.ColumnSet = new ColumnSet(new string[] { "new_hopdongdautumia", "new_thuadat", "new_name" });
                q.Criteria = new FilterExpression(LogicalOperator.And);
                q.Criteria.AddCondition(new ConditionExpression("new_dientichconlai", ConditionOperator.NotEqual, new decimal(0)));
                q.Criteria.AddCondition(new ConditionExpression("statuscode", ConditionOperator.NotEqual, 100000007));
                LinkEntity linkEntity1 = new LinkEntity("new_thuadatcanhtac", "new_hopdongdautumia", "new_hopdongdautumia", "new_hopdongdautumiaid", JoinOperator.Inner);
                q.LinkEntities.Add(linkEntity1);
                linkEntity1.LinkCriteria = new FilterExpression();
                linkEntity1.LinkCriteria.AddCondition(new ConditionExpression("new_vudautu", ConditionOperator.Equal, ((EntityReference)hopdongdautumia["new_vudautu"]).Id));
                linkEntity1.LinkCriteria.AddCondition(new ConditionExpression("new_hoanthanhhopdong", ConditionOperator.Equal, false));

                q.Criteria = new FilterExpression(LogicalOperator.And);
                q.Criteria.AddCondition(new ConditionExpression("statuscode", ConditionOperator.Equal, 100000000));
                EntityCollection entc = service.RetrieveMultiple(q);                
                List<Entity> lstThuadatcanhtac = entc.Entities.ToList();

                foreach (Entity en in lstThuadatcanhtac)
                {
                    Guid id = en.Contains("new_thuadat") ? ((EntityReference)en["new_thuadat"]).Id : Guid.NewGuid();

                    if (thuadat.Id == id)                    
                        count1++;                    
                }

                if (statuscode == "100000000")
                {
                    if (count1 > 1)                    
                        throw new Exception("Thửa đất đã tồn tại trong hợp đồng khác !!!");                    
                }
                else
                {
                    if (count1 > 0)                    
                        throw new Exception("Thửa đất đã tồn tại trong hợp đồng khác !!!");
                    
                }

                if (hopdongdautumia.Contains("new_quocgia") && thuadat.Contains("new_diachi"))
                {
                    Entity diachi = service.Retrieve("new_diachi", ((EntityReference)thuadat["new_diachi"]).Id,
                        new ColumnSet(new string[] { "new_quocgia" }));

                    if (((EntityReference)hopdongdautumia["new_quocgia"]).Id != ((EntityReference)diachi["new_quocgia"]).Id)
                    {
                        throw new Exception("Quốc gia của thửa đất và hợp đồng tư mía không giống nhau !! ");
                    }
                }
            }

            if (context.MessageName == "Update")
            {
                //if (target.Contains("new_hopdongdautumia"))
                //{
                //    Entity hddtm = service.Retrieve("new_hopdongdautumia", ((EntityReference)target["new_hopdongdautumia"]).Id, new ColumnSet
                //        (new string[] { "new_hopdongdautumiaid", "statuscode" }));

                //    if ((hddtm.Contains("statuscode") && ((OptionSetValue)hddtm["statuscode"]).Value == 100000003) && (!target.Contains("new_phuluchopdongid")))
                //    {
                //        throw new Exception("Hợp đồng đã ký không được phép thêm chi tiết hợp đồng đầu tư mía !!!");
                //    }
                //}

                if (target.Contains("new_thuadat"))
                {
                    Entity thuadatcanhtac = service.Retrieve(target.LogicalName, target.Id,
                        new ColumnSet(new string[] { "new_thuadat", "new_hopdongdautumia" }));

                    Entity thuadat = service.Retrieve("new_thuadat", ((EntityReference)thuadatcanhtac["new_thuadat"]).Id,
                        new ColumnSet(new string[] { "new_diachi" }));

                    if (!thuadatcanhtac.Contains("new_hopdongdautumia"))                    
                        throw new Exception("Chưa chọn hợp đồng tư mía !! ");                    

                    Entity hopdongdatumia = service.Retrieve("new_hopdongdautumia", ((EntityReference)thuadatcanhtac["new_hopdongdautumia"]).Id, new ColumnSet(true));
                    if (hopdongdatumia.Contains("new_quocgia") && thuadat.Contains("new_diachi"))
                    {
                        Entity diachi = service.Retrieve("new_diachi", ((EntityReference)thuadat["new_diachi"]).Id, new ColumnSet(new string[] { "new_quocgia" }));

                        if (((EntityReference)hopdongdatumia["new_quocgia"]).Id != ((EntityReference)diachi["new_quocgia"]).Id)                        
                            throw new Exception("Quốc gia của thửa đất và hợp đồng tư mía không giống nhau !! ");
                        
                    }
                }
            }
        }

        List<Entity> RetrieveMultiRecord(IOrganizationService crmservices, string entity, ColumnSet column, string condition, object value)
        {
            QueryExpression q = new QueryExpression(entity);
            q.ColumnSet = column;
            q.Criteria = new FilterExpression(LogicalOperator.And);
            q.Criteria.AddCondition(new ConditionExpression(condition, ConditionOperator.Equal, value));
            q.Criteria.AddCondition(new ConditionExpression("statuscode", ConditionOperator.NotEqual, 100000007));
            EntityCollection entc = service.RetrieveMultiple(q);

            return entc.Entities.ToList<Entity>();
        }
    }
}
