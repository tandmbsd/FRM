using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk.Messages;

namespace Plugin_DuyetNghiemThuTrongMia
{
    public class Plugin_DuyetNghiemThuTrongMia : IPlugin
    {
        IOrganizationService service = null;
        IOrganizationServiceFactory factory = null;
        void IPlugin.Execute(IServiceProvider serviceProvider)
        {
            IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
            service = factory.CreateOrganizationService(context.UserId);
            
            Entity target = (Entity)context.InputParameters["Target"];
            if (target.Contains("statuscode"))
            {
                if (((OptionSetValue)target["statuscode"]).Value == 100000000)
                {
                    List<Entity> dsctNghiemThu = RetrieveMultiRecord(service, "new_chitietnghiemthutrongmia", new ColumnSet(true), "new_nghiemthutrongmia", target.Id);
                    foreach (Entity a in dsctNghiemThu)
                    {
                        Entity Giong = service.Retrieve("new_giongmia", ((EntityReference)a["new_giongmia"]).Id, new ColumnSet(true));
                        Entity chitietHD = GetChiTietHD(service, ((EntityReference)a["new_thuadat"]).Id, target.Id);
                        if (chitietHD == null)
                            throw new Exception("Không tìm thấy chi tiết HĐ đầu tư mía trên thửa đất này !");
                        Entity up = new Entity("new_thuadatcanhtac");
                        up.Id = chitietHD.Id;

                        up["new_dientichthucte"] = (a.Attributes.Contains("new_dientichnghiemthu") ? (decimal)a["new_dientichnghiemthu"] : 0);
                        up["new_dientichconlai"] = (a.Attributes.Contains("new_dientichnghiemthu") ? (decimal)a["new_dientichnghiemthu"] : 0);
                        up["new_giongtrongthucte"] = a.Attributes.Contains("new_giongmia") ? a["new_giongmia"] : null;
                        if (a.Contains("new_ngaytrongxulygoc"))
                        {
                            up["new_ngaytrong"] = a.Attributes.Contains("new_ngaytrongxulygoc") ? a["new_ngaytrongxulygoc"] : null;
                            if (((OptionSetValue)chitietHD["new_loaigocmia"]).Value == 100000000)
                                up["new_ngaythuhoachdukien"] = ((DateTime)a["new_ngaytrongxulygoc"]).AddMonths(Giong.Contains("new_tuoichinmiato") ? (int)Giong["new_tuoichinmiato"] : 0);
                            else
                                up["new_ngaythuhoachdukien"] = ((DateTime)a["new_ngaytrongxulygoc"]).AddMonths(Giong.Contains("new_tuoichinmiagoc") ? (int)Giong["new_tuoichinmiagoc"] : 0);
                        }

                        service.Update(up);
                    }
                }
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

        public static Entity GetChiTietHD(IOrganizationService service, Guid ThuadatId, Guid NTTrongId)
        {
            QueryExpression q = new QueryExpression("new_thuadatcanhtac");
            q.ColumnSet = new ColumnSet(true);
            LinkEntity l1 = new LinkEntity("new_thuadatcanhtac", "new_hopdongdautumia", "new_hopdongdautumia", "new_hopdongdautumiaid", JoinOperator.Inner);
            LinkEntity l2 = new LinkEntity("new_hopdongdautumia", "new_nghiemthutrongmia", "new_hopdongdautumiaid", "new_hopdongtrongmia", JoinOperator.Inner);
            l2.LinkCriteria.AddCondition(new ConditionExpression("activityid", ConditionOperator.Equal, NTTrongId));
            l1.LinkEntities.Add(l2);
            q.LinkEntities.Add(l1);
            q.Criteria.AddCondition(new ConditionExpression("new_thuadat", ConditionOperator.Equal, ThuadatId));

            EntityCollection result = service.RetrieveMultiple(q);
            if (result.Entities.Count > 0)
                return result.Entities[0];
            else return null;

        }
    }
}
