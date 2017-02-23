using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk.Messages;

namespace Plugin_PGNPhanBon
{
    public class Plugin_PGNPhanBon : IPlugin
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
                Entity PGNHG = service.Retrieve(target.LogicalName, target.Id, new ColumnSet(true));
                string loaigiaonhanhom = ((OptionSetValue)PGNHG["new_loaigiaonhanhom"]).Value.ToString();
                decimal tongtien = PGNHG.Contains("new_tongtien") ? ((Money)PGNHG["new_tongtien"]).Value : 0;
                EntityCollection chitiethddtm = RetrieveNNRecord(service, "new_thuadatcanhtac", "new_phieugiaonhanhomgiong", "new_new_pgnhomgiong_new_chitiethddtmia", new ColumnSet(new string[] { "new_khachhang", "new_khachhangdoanhnghiep", "new_hopdongdautumia", "new_dientichthucte", "new_chinhsachdautu", "new_no", "new_co", "new_name" }), "new_phieugiaonhanhomgiongid", PGNHG.Id);
                decimal sotienthat = 0;
                Guid id = new Guid();

                foreach (Entity en in chitiethddtm.Entities)
                {
                    if (!en.Contains("new_chinhsachdautu"))
                    {
                        throw new Exception(en["new_name"].ToString() + " chưa có chính sách đầu tư");
                    }

                    Entity chinhsachdautu = service.Retrieve("new_chinhsachdautu", ((EntityReference)en["new_chinhsachdautu"]).Id, new ColumnSet(true));
                    List<Entity> tilethuhoi = RetrieveMultiRecord(service, "new_tilethuhoivon", new ColumnSet(new string[] { "new_phantramtilethuhoi", "new_nam" }), "new_chinhsachdautu", chinhsachdautu.Id);
                    Entity phanbodautu = new Entity("new_phanbodautu");

                    if (PGNHG.Contains("new_khachhang"))
                    {
                        phanbodautu["new_khachhang"] = PGNHG["new_khachhang"];
                    }
                    else
                    {
                        phanbodautu["new_khachhangdoanhnghiep"] = PGNHG["new_khachhangdoanhnghiep"];
                    }

                    phanbodautu["new_loaihopdong"] = new OptionSetValue(100000001);
                    phanbodautu["new_thuacanhtac"] = en.ToEntityReference();

                    Entity vudautu = service.Retrieve("new_vudautu", ((EntityReference)PGNHG["new_vudautu"]).Id, new ColumnSet(true));
                    phanbodautu["new_hopdongdautumia"] = en["new_hopdongdautumia"];
                    phanbodautu["new_vudautu"] = PGNHG["new_vudautu"];
                    //phanbodautu["new_phieuchitienmat"] = new EntityReference(target.LogicalName, target.Id);
                    phanbodautu["new_ngayphatsinh"] = DateTime.Now;
                    phanbodautu["new_datra"] = new Money(0);
                    phanbodautu["new_loaiphanbo"] = new OptionSetValue(100000003);
                    //phanbodautu["new_loaidautu"] = ((OptionSetValue)ct["new_loaidautu"]).Value.ToString() == "100000000" ? new OptionSetValue(100000001) : new OptionSetValue(100000000);

                    if (tilethuhoi.Count == 0)
                    {
                        throw new Exception("Chính sách không có tỉ lệ thu hồi !!!");
                    }

                    foreach (Entity tlth in tilethuhoi)
                    {
                        Money k = new Money(Math.Ceiling((tongtien / sum(chitiethddtm))) * en.GetAttributeValue<decimal>("new_dientichthucte") * tlth.GetAttributeValue<decimal>("new_phantramtilethuhoi") / new decimal(100));
                        phanbodautu["new_sotien"] = k;
                        sotienthat += k.Value;

                        DateTime dt = ((DateTime)vudautu["new_ngayketthucvuthuhoach"]).ToLocalTime();

                        phanbodautu["new_hanthanhtoan"] = new DateTime(tlth.GetAttributeValue<int>("new_nam"), dt.Month, dt.Day);

                        id = service.Create(phanbodautu);

                        Entity phanbodautu1 = service.Retrieve("new_phanbodautu", id, new ColumnSet(new string[] { "new_maphieuphanbo", "new_name", "new_datra", "new_conlai", "new_sotien", "new_hanthanhtoan" }));

                        phanbodautu1["new_name"] = "PBĐT" + phanbodautu1["new_maphieuphanbo"].ToString();
                        phanbodautu1["new_conlai"] = new Money((decimal)((Money)phanbodautu1["new_sotien"]).Value - (decimal)((Money)phanbodautu1["new_datra"]).Value);
                        DateTime dtStart = ((DateTime)vudautu["new_ngaybatdau"]).ToLocalTime();

                        int cp1 = CompareDatetime(phanbodautu.GetAttributeValue<DateTime>("new_hanthanhtoan"), dtStart);
                        int cp2 = CompareDatetime(phanbodautu.GetAttributeValue<DateTime>("new_hanthanhtoan"), dt);
                        if (0 <= cp1 && cp2 <= 0)
                        {
                            phanbodautu1["new_chuyenno"] = false;
                        }
                        else
                        {
                            phanbodautu1["new_chuyenno"] = true;
                        }
                        service.Update(phanbodautu1);
                        if (loaigiaonhanhom == "100000001") // nong dan - nong dan
                        {
                            if (!PGNHG.Contains("new_chitiethddtmia_doitac"))
                                throw new Exception("Bạn chưa điền thông tin chi tiết đối tác giao hôm trong PGN hôm giống !!! ");
                            Entity chitietdoitacgiaohom = service.Retrieve("new_thuadatcanhtac", ((EntityReference)PGNHG["new_chitiethddtmia_doitac"]).Id, new ColumnSet(new string[] { "new_name", "new_no", "new_co" }));

                            if (!chitietdoitacgiaohom.Contains("new_no"))
                            {
                                chitietdoitacgiaohom["new_no"] = new Money(0);
                            }

                            chitietdoitacgiaohom["new_no"] = new Money(((Money)phanbodautu1["new_sotien"]).Value + ((Money)chitietdoitacgiaohom["new_no"]).Value);
                            service.Update(chitietdoitacgiaohom);
                        }

                        if (en.Contains("new_co") == false)
                        {
                            en["new_co"] = new Money(0);
                        }

                        en["new_co"] = new Money(((Money)phanbodautu1["new_sotien"]).Value + ((Money)en["new_co"]).Value);
                        service.Update(en);
                    }
                }
            }
        }
        int CompareDatetime(DateTime t1, DateTime t2)
        {
            int flag;
            if (t1.Year < t2.Year)
            {
                flag = -1;
            }
            else if (t1.Year == t2.Year)
            {
                if (t1.Month < t2.Month)
                {
                    flag = -1;
                }
                else if (t1.Month == t2.Month)
                {
                    if (t1.Day < t2.Day)
                    {
                        flag = -1;
                    }
                    else if (t1.Day == t2.Day)
                    {
                        flag = 0;
                    }
                    else
                        flag = 1;
                }
                else
                    flag = 1;
            }
            else
                flag = 1;


            return flag;
        }
        decimal sum(EntityCollection thuacanhtac)
        {
            decimal temp = 0;
            foreach (Entity en in thuacanhtac.Entities)
            {
                temp += en.Contains("new_dientichthucte") ? en.GetAttributeValue<decimal>("new_dientichthucte") : 0;
            }

            return temp;
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
        EntityCollection RetrieveNNRecord(IOrganizationService crmservices, string entity1, string entity2, string relateName, ColumnSet column, string condition, object value)
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
            EntityCollection collRecords = service.RetrieveMultiple(query);

            return collRecords;
        }
    }
}
