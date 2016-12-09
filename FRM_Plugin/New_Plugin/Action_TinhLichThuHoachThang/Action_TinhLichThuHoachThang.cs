using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk.Messages;
using Microsoft.Crm.Sdk.Messages;

namespace Action_TinhLichThuHoachThang
{
    public class Action_TinhLichThuHoachThang : IPlugin
    {
        IOrganizationService service = null;
        IOrganizationServiceFactory factory = null;
        void IPlugin.Execute(IServiceProvider serviceProvider)
        {
            IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            EntityReference target = (EntityReference)context.InputParameters["Target"];
            factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
            service = factory.CreateOrganizationService(context.UserId);

            Entity Vuthuhoach = service.Retrieve(target.LogicalName, target.Id, new ColumnSet(true));
            Entity Vudautu = service.Retrieve("new_vudautu", ((EntityReference)Vuthuhoach["new_vudautu"]).Id, new ColumnSet(new string[] { "new_namtaichinhh" }));
            if (!Vudautu.Contains("new_namtaichinhh"))
                throw new Exception("Không tìm thấy năm tài chính !");
            int nam = int.Parse(Vudautu["new_namtaichinhh"].ToString()) + 1;

            if (Vuthuhoach != null)
            {
                ////delete all lich thu hoach cu
                QueryExpression qe = new QueryExpression("new_lichthuhoachthang");
                qe.ColumnSet = new ColumnSet(new string[] { "new_lichthuhoachthangid" });
                qe.Criteria.AddCondition(new ConditionExpression("new_vuthuhoach", ConditionOperator.Equal, target.Id));
                EntityCollection en = service.RetrieveMultiple(qe);
                foreach (Entity a in en.Entities)
                    service.Delete(a.LogicalName, a.Id);

                //delete all dot thu hoach cu
                qe = new QueryExpression("new_dotthuhoach");
                qe.ColumnSet = new ColumnSet(new string[] { "new_dotthuhoachid" });
                qe.Criteria.AddCondition(new ConditionExpression("new_vuthuhoach", ConditionOperator.Equal, target.Id));
                EntityCollection ens = service.RetrieveMultiple(qe);
                foreach (Entity a in ens.Entities)
                    service.Delete(a.LogicalName, a.Id);

                //Get Doan thu hoach
                qe = new QueryExpression("new_giaidoanthuhoach");
                qe.ColumnSet = new ColumnSet(new string[] { "new_tungay", "new_denngay" });
                qe.Criteria.AddCondition(new ConditionExpression("new_vuthuhoach", ConditionOperator.Equal, target.Id));
                qe.Orders.Add(new OrderExpression("new_tungay", OrderType.Ascending));
                EntityCollection doanth = service.RetrieveMultiple(qe);
                List<DoanTH> DSDoanTH = new List<DoanTH>();
                foreach (Entity a in doanth.Entities)
                {
                    DoanTH tmp = new DoanTH();
                    tmp.id = a.Id;
                    tmp.tu = ((DateTime)a["new_tungay"]).AddHours(7).Date;
                    tmp.den = ((DateTime)a["new_denngay"]).AddHours(7).Date;
                    DSDoanTH.Add(tmp);
                }

                //Get Lich ngung thu hoach
                qe = new QueryExpression("new_lichngungthuhoach");
                qe.ColumnSet = new ColumnSet(new string[] { "new_tungay", "new_denngay" });
                qe.Criteria.AddCondition(new ConditionExpression("new_vuthuhoach", ConditionOperator.Equal, target.Id));
                qe.Orders.Add(new OrderExpression("new_tungay", OrderType.Ascending));
                EntityCollection ent = service.RetrieveMultiple(qe);
                List<NgungTH> DSNgungTH = new List<NgungTH>();
                foreach (Entity a in ent.Entities)
                {
                    NgungTH tmp = new NgungTH();
                    tmp.tu = ((DateTime)a["new_tungay"]).AddHours(7).Date;
                    tmp.den = ((DateTime)a["new_denngay"]).AddHours(7).Date;
                    DSNgungTH.Add(tmp);
                }

                int i = 1;
                foreach (DoanTH c in DSDoanTH)
                {
                    //Gen dot thu hoach
                    DateTime ngaybatdau = c.tu.AddDays(-1);
                    DateTime ngayketthuc = c.den;
                    DateTime index = ngaybatdau;
                    Entity dotth = new Entity("new_dotthuhoach");

                    while (index < ngayketthuc)
                    {
                        bool co = false;
                        if (DSNgungTH.Count > 0)
                            if (index.AddDays(1) >= DSNgungTH[0].tu)
                            {
                                index = DSNgungTH[0].den;
                                DSNgungTH.RemoveAt(0);
                            }
                        dotth["new_tungay"] = index.AddDays(1);

                        DateTime denngay;
                        bool co1 = true;
                        if (index.AddDays(10) >= ngayketthuc)
                        {
                            co1 = false;
                            denngay = ngayketthuc;
                        }
                        else denngay = index.AddDays(10);

                        if (co1 && ((index.AddDays(1).Month != denngay.Month) || (index.AddDays(1).Day >= 20 && denngay.Day < DateTime.DaysInMonth(denngay.Year, denngay.Month))))
                            denngay = new DateTime(index.AddDays(1).Year, index.AddDays(1).Month, DateTime.DaysInMonth(index.AddDays(1).Year, index.AddDays(1).Month));

                        if (DSNgungTH.Count > 0)
                        {
                            if (denngay >= DSNgungTH[0].tu)
                            {
                                denngay = DSNgungTH[0].tu.AddDays(-1);
                                index = DSNgungTH[0].den;
                                DSNgungTH.RemoveAt(0);
                                co = true;
                            }
                        }

                        dotth["new_denngay"] = denngay;
                        dotth["new_name"] = "Đợt " + i.ToString();
                        dotth["new_songayhoatdong"] = (denngay - (DateTime)dotth["new_tungay"]).Days + 1;
                        dotth["new_vuthuhoach"] = target;
                        dotth["new_giaidoanthuhoach"] = new EntityReference("new_giaidoanthuhoach", c.id);

                        service.Create(dotth);
                        dotth = new Entity("new_dotthuhoach");
                        if (!co)
                            index = denngay;
                        i++;
                    }
                }

                //Query all thuadatcanhtac

                Dictionary<Guid, LichThang> data = new Dictionary<Guid, LichThang>();

                qe = new QueryExpression("new_thuadatcanhtac");
                qe.ColumnSet = new ColumnSet(new string[] { "new_name", "new_ngaytrong", "new_tram", "new_khachhang", "new_khachhangdoanhnghiep", "new_ngaythuhoachdukien", "new_ngaydukienthuhoachsom", "new_lydothuhoachsom", "new_sanluonguoc", "new_loaigocmia", "new_luugoc", "new_ngayktbonphan", "new_tuoimia" });

                LinkEntity l1 = new LinkEntity("new_thuadatcanhtac", "new_hopdongdautumia", "new_hopdongdautumia", "new_hopdongdautumiaid", JoinOperator.Inner);
                l1.LinkCriteria.AddCondition(new ConditionExpression("new_vudautu", ConditionOperator.Equal, ((EntityReference)Vuthuhoach["new_vudautu"]).Id));
                qe.LinkEntities.Add(l1);

                qe.Criteria.AddCondition(new ConditionExpression("new_sanluonguoc", ConditionOperator.GreaterThan, (decimal)0));
                qe.Criteria.AddCondition(new ConditionExpression("statuscode", ConditionOperator.Equal, 100000000));
                qe.Criteria.AddCondition(new ConditionExpression("new_ngaytrong", ConditionOperator.NotNull));
                qe.Criteria.AddCondition(new ConditionExpression("new_dientichconlai", ConditionOperator.GreaterThan, (decimal)0));

                qe.PageInfo = new PagingInfo();
                qe.PageInfo.Count = 5000;
                qe.PageInfo.PageNumber = 1;
                qe.PageInfo.PagingCookie = null;

                while (true)
                {
                    EntityCollection result = service.RetrieveMultiple(qe);

                    foreach (Entity a in result.Entities)
                    {
                        if (a.Contains("new_tram") && a.Contains("new_ngaythuhoachdukien") && (a.Contains("new_khachhang") || a.Contains("new_khachhangdoanhnghiep")))
                        {
                            DateTime date = (a.Contains("new_ngaydukienthuhoachsom") ? (DateTime)a["new_ngaydukienthuhoachsom"] : (a.Contains("new_ngaydukienthuhoach") ? (DateTime)a["new_ngaydukienthuhoach"] : (DateTime)a["new_ngaythuhoachdukien"]));
                            date = date.AddHours(7).Date;

                            if (data.ContainsKey(((EntityReference)a["new_tram"]).Id)) //da ton tai
                            {
                                if (date <= new DateTime(nam, 11, DateTime.DaysInMonth(nam, 11)))
                                    data[((EntityReference)a["new_tram"]).Id].Thang11 += Decimal.ToInt32((a.Contains("new_sanluonguoc") ? (decimal)a["new_sanluonguoc"] : (decimal)0));
                                else if (date > new DateTime(nam, 11, DateTime.DaysInMonth(nam, 11)) && date <= new DateTime(nam, 12, DateTime.DaysInMonth(nam, 12)))
                                    data[((EntityReference)a["new_tram"]).Id].Thang12 += Decimal.ToInt32((a.Contains("new_sanluonguoc") ? (decimal)a["new_sanluonguoc"] : (decimal)0));
                                else if (date > new DateTime(nam, 12, DateTime.DaysInMonth(nam, 12)) && date <= new DateTime(nam + 1, 1, DateTime.DaysInMonth(nam + 1, 1)))
                                    data[((EntityReference)a["new_tram"]).Id].Thang1 += Decimal.ToInt32((a.Contains("new_sanluonguoc") ? (decimal)a["new_sanluonguoc"] : (decimal)0));
                                else if (date > new DateTime(nam + 1, 1, DateTime.DaysInMonth(nam + 1, 1)) && date <= new DateTime(nam + 1, 2, DateTime.DaysInMonth(nam + 1, 2)))
                                    data[((EntityReference)a["new_tram"]).Id].Thang2 += Decimal.ToInt32((a.Contains("new_sanluonguoc") ? (decimal)a["new_sanluonguoc"] : (decimal)0));
                                else if (date > new DateTime(nam + 1, 2, DateTime.DaysInMonth(nam + 1, 2)) && date <= new DateTime(nam + 1, 3, DateTime.DaysInMonth(nam + 1, 3)))
                                    data[((EntityReference)a["new_tram"]).Id].Thang3 += Decimal.ToInt32((a.Contains("new_sanluonguoc") ? (decimal)a["new_sanluonguoc"] : (decimal)0));
                                else
                                    data[((EntityReference)a["new_tram"]).Id].Thang4 += Decimal.ToInt32((a.Contains("new_sanluonguoc") ? (decimal)a["new_sanluonguoc"] : (decimal)0));
                            }
                            else //chua co tram
                            {
                                LichThang tmp = new LichThang();
                                tmp.Tram = ((EntityReference)a["new_tram"]).Id;
                                if (date <= new DateTime(nam, 11, DateTime.DaysInMonth(nam, 11)))
                                    tmp.Thang11 += Decimal.ToInt32((a.Contains("new_sanluonguoc") ? (decimal)a["new_sanluonguoc"] : (decimal)0));
                                else if (date > new DateTime(nam, 11, DateTime.DaysInMonth(nam, 11)) && date <= new DateTime(nam, 12, DateTime.DaysInMonth(nam, 12)))
                                    tmp.Thang12 += Decimal.ToInt32((a.Contains("new_sanluonguoc") ? (decimal)a["new_sanluonguoc"] : (decimal)0));
                                else if (date > new DateTime(nam, 12, DateTime.DaysInMonth(nam, 12)) && date <= new DateTime(nam + 1, 1, DateTime.DaysInMonth(nam + 1, 1)))
                                    tmp.Thang1 += Decimal.ToInt32((a.Contains("new_sanluonguoc") ? (decimal)a["new_sanluonguoc"] : (decimal)0));
                                else if (date > new DateTime(nam + 1, 1, DateTime.DaysInMonth(nam + 1, 1)) && date <= new DateTime(nam + 1, 2, DateTime.DaysInMonth(nam + 1, 2)))
                                    tmp.Thang2 += Decimal.ToInt32((a.Contains("new_sanluonguoc") ? (decimal)a["new_sanluonguoc"] : (decimal)0));
                                else if (date > new DateTime(nam + 1, 2, DateTime.DaysInMonth(nam + 1, 2)) && date <= new DateTime(nam + 1, 3, DateTime.DaysInMonth(nam + 1, 3)))
                                    tmp.Thang3 += Decimal.ToInt32((a.Contains("new_sanluonguoc") ? (decimal)a["new_sanluonguoc"] : (decimal)0));
                                else
                                    tmp.Thang4 += Decimal.ToInt32((a.Contains("new_sanluonguoc") ? (decimal)a["new_sanluonguoc"] : (decimal)0));
                                data.Add(((EntityReference)a["new_tram"]).Id, tmp);
                            }
                        }
                    }

                    if (result.MoreRecords)
                    {
                        qe.PageInfo.PageNumber++;
                        qe.PageInfo.PagingCookie = result.PagingCookie;
                    }
                    else {
                        break;
                    }
                }

                foreach (KeyValuePair<Guid, LichThang> entry in data)
                {
                    Entity a = new Entity("new_lichthuhoachthang");
                    a["new_vuthuhoach"] = target;
                    a["new_tram"] = new EntityReference("businessunit", entry.Key);
                    a["new_vudautu"] = Vudautu.ToEntityReference();
                    a["new_loai"] = new OptionSetValue(100000000);
                    a["new_thang11"] = entry.Value.Thang11;
                    a["new_thang12"] = entry.Value.Thang12;
                    a["new_thang1"] = entry.Value.Thang1;
                    a["new_thang2"] = entry.Value.Thang2;
                    a["new_thang3"] = entry.Value.Thang3;
                    a["new_thang4"] = entry.Value.Thang4;
                    a["new_tongcong"] = entry.Value.Thang11 + entry.Value.Thang12 + entry.Value.Thang1 + entry.Value.Thang2 + entry.Value.Thang3 + entry.Value.Thang4;
                    a["new_tongbandau"] = entry.Value.Thang11 + entry.Value.Thang12 + entry.Value.Thang1 + entry.Value.Thang2 + entry.Value.Thang3 + entry.Value.Thang4;
                    service.Create(a);

                    a["new_loai"] = new OptionSetValue(100000001);
                    service.Create(a);
                }

                context.OutputParameters["Return"] = "success";
            }
            else
            {
                context.OutputParameters["Return"] = "Không tìm thấy Vụ thu hoạch, vui lòng save form trước khi chạy tính năng !";
            }
        }
    }
}
