using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Action_CopyBangGia
{
    public class ActionCopy_BangGiaCongDon : IPlugin
    {
        IOrganizationService service = null;
        IOrganizationServiceFactory factory = null;
        void IPlugin.Execute(IServiceProvider serviceProvider)
        {
            IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
            service = factory.CreateOrganizationService(context.UserId);

            EntityReference target = (EntityReference)context.InputParameters["Target"];

            string input = (string)context.InputParameters["VudautuID"];
            Guid vudautuid = new Guid(input);

            Entity vudautu = service.Retrieve("new_vudautu", vudautuid, new ColumnSet(new string[] { "new_vudautuid" }));

            if (target.LogicalName == "new_banggiacongdon")
            {
                Entity bgcd = service.Retrieve(target.LogicalName, target.Id, new ColumnSet(true));

                if (bgcd == null)
                {
                    throw new Exception("Bảng giá công đốn này không tồn tại !!");
                }

                Entity new_bgcd = new Entity("new_banggiacongdon");

                string newName = bgcd.Contains("new_name") ? bgcd["new_name"].ToString() : "";
                new_bgcd["new_name"] = "New - " + newName;
                new_bgcd["new_ngayapdung"] = bgcd["new_ngayapdung"];
                new_bgcd["new_vudautu"] = new EntityReference("new_vudautu", vudautu.Id);
                new_bgcd["statuscode"] = new OptionSetValue(1);
                Guid new_bgcdID = service.Create(new_bgcd);

                List<Entity> lstChitietbanggiacongdon = RetrieveMultiRecord(service, "new_chitietbanggiacongdon", new ColumnSet(true), "new_banggiacongdon", bgcd.Id);
                if (lstChitietbanggiacongdon.Count > 0)
                {
                    foreach (Entity en in lstChitietbanggiacongdon)
                    {
                        Entity new_Chitietbgcd = new Entity("new_chitietbanggiacongdon");
                        new_Chitietbgcd["new_name"] = en["new_name"];
                        new_Chitietbgcd["new_ngayapdung"] = en["new_ngayapdung"];

                        Entity bgcdEn = new Entity("new_banggiacongdon");
                        bgcdEn.Id = new_bgcdID;
                        new_Chitietbgcd["new_banggiacongdon"] = bgcdEn.ToEntityReference();

                        new_Chitietbgcd["new_nhomnangsuat"] = en["new_nhomnangsuat"];
                        new_Chitietbgcd["new_giacongdon"] = en["new_giacongdon"];
                        new_Chitietbgcd["new_giatrungchuyenduoi500m"] = en["new_giatrungchuyenduoi500m"];
                        new_Chitietbgcd["new_giatrungchuyentren500m"] = en["new_giatrungchuyentren500m"];
                        service.Create(new_Chitietbgcd);
                    }
                }
                
                List<Entity> lstTanggiamgiacongdon = RetrieveMultiRecord(service, "new_tanggiamgiacongdontheovung", new ColumnSet(true), "new_banggiacongdon", bgcd.Id);
                if (lstTanggiamgiacongdon.Count > 0)
                {
                    foreach (Entity en in lstTanggiamgiacongdon)
                    {
                        Entity new_tanggiamgiacongdon = new Entity("new_tanggiamgiacongdontheovung");
                        new_tanggiamgiacongdon["new_name"] = en["new_name"];

                        Entity bgcdEn = new Entity("new_banggiacongdon");
                        bgcdEn.Id = new_bgcdID;
                        new_tanggiamgiacongdon["new_banggiacongdon"] = bgcdEn.ToEntityReference();
                        new_tanggiamgiacongdon["new_ngayapdung"] = en["new_ngayapdung"];
                        new_tanggiamgiacongdon["new_vungdialy"] = en["new_vungdialy"];
                        new_tanggiamgiacongdon["new_sotientanggiam"] = en["new_sotientanggiam"];
                        service.Create(new_tanggiamgiacongdon);
                    }
                }
                
                List<Entity> lstTanggiamgiatrungchuyen = RetrieveMultiRecord(service, "new_tanggiamgiatrungchuyentheovung", new ColumnSet(true), "new_banggiacongdon", bgcd.Id);

                if (lstTanggiamgiatrungchuyen.Count > 0)
                {
                    foreach (Entity en in lstTanggiamgiatrungchuyen)
                    {
                        Entity t = new Entity("new_tanggiamgiatrungchuyentheovung");
                        t["new_name"] = en["new_name"];
                        
                        Entity bgcdEn = new Entity("new_banggiacongdon");
                        bgcdEn.Id = new_bgcdID;
                        t["new_banggiacongdon"] = bgcdEn.ToEntityReference();
                        t["new_ngayapdung"] = en["new_ngayapdung"];
                        t["new_vungdialy"] = en["new_vungdialy"];
                        t["new_heso"] = en["new_heso"];
                        service.Create(t);
                    }
                }
                
                context.OutputParameters["ReturnId"] = new_bgcdID.ToString();
            }
        }

        EntityReference FindVudautu(EntityReference vudautuRef)
        {
            Entity CurrVudautu = service.Retrieve("new_vudautu", vudautuRef.Id, new ColumnSet(true));
            QueryExpression q = new QueryExpression("new_vudautu");
            q.ColumnSet = new ColumnSet(true);
            q.Criteria = new FilterExpression();
            q.Criteria.AddCondition(new ConditionExpression("statecode", ConditionOperator.Equal, 0));
            EntityCollection entc = service.RetrieveMultiple(q);

            List<Entity> lst = entc.Entities.OrderBy(p => p.GetAttributeValue<DateTime>("new_ngaybatdau")).ToList<Entity>();
            int curr = lst.FindIndex(p => p.Id == CurrVudautu.Id);
            if (curr == lst.Count - 1)
            {
                throw new Exception("Không tồn tại vụ đầu tư mới hơn !!!");
            }
            return lst[curr + 1].ToEntityReference();

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
