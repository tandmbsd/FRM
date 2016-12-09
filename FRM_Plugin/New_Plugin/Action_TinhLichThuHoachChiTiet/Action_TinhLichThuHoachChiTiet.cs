using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk.Messages;
using Microsoft.Crm.Sdk.Messages;

namespace Action_TinhLichThuHoachChiTiet
{
    public class Action_TinhLichThuHoachChiTiet : IPlugin
    {
        IOrganizationService service = null;
        IOrganizationServiceFactory factory = null;
        void IPlugin.Execute(IServiceProvider serviceProvider)
        {
            IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            EntityReference target = (EntityReference)context.InputParameters["Target"];
            factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
            service = factory.CreateOrganizationService(context.UserId);
            string vl = "";

            try
            {
                Entity Vuthuhoach = service.Retrieve(target.LogicalName, target.Id, new ColumnSet(true));
                Entity Vudautu = service.Retrieve("new_vudautu", ((EntityReference)Vuthuhoach["new_vudautu"]).Id, new ColumnSet(new string[] { "new_namtaichinhh" }));
                if (!Vudautu.Contains("new_namtaichinhh"))
                    throw new Exception("Không tìm thấy năm tài chính !");
                int nam = int.Parse(Vudautu["new_namtaichinhh"].ToString()) + 1;
                if (Vuthuhoach != null)
                {
                    //delete all lich thu hoach cu
                    QueryExpression qe = new QueryExpression("new_lichthuhoach10ngay");
                    qe.ColumnSet = new ColumnSet(new string[] { "new_lichthuhoach10ngayid" });
                    qe.Criteria.AddCondition(new ConditionExpression("new_vuthuhoach", ConditionOperator.Equal, target.Id));
                    EntityCollection en = service.RetrieveMultiple(qe);
                    foreach (Entity a in en.Entities)
                        service.Delete(a.LogicalName, a.Id);

                    //get DS tram thu hoach
                    Dictionary<Guid, LichThang> DSTramTH = new Dictionary<Guid, LichThang>();
                    qe = new QueryExpression("new_lichthuhoachthang");
                    qe.ColumnSet = new ColumnSet(new string[] { "new_tram", "new_thang11", "new_thang12", "new_thang1", "new_thang2", "new_thang3", "new_thang4" });
                    qe.Criteria.AddCondition(new ConditionExpression("new_vuthuhoach", ConditionOperator.Equal, target.Id));
                    qe.Criteria.AddCondition(new ConditionExpression("new_loai", ConditionOperator.Equal, 100000001));
                    EntityCollection ens = service.RetrieveMultiple(qe);
                    foreach (Entity a in ens.Entities)
                    {
                        LichThang tmp = new LichThang();
                        tmp.Tram = ((EntityReference)a["new_tram"]).Id;
                        tmp.Thang11 = (int)a["new_thang11"];
                        tmp.Thang12 = (int)a["new_thang12"];
                        tmp.Thang1 = (int)a["new_thang1"];
                        tmp.Thang2 = (int)a["new_thang2"];
                        tmp.Thang3 = (int)a["new_thang3"];
                        tmp.Thang4 = (int)a["new_thang4"];
                        DSTramTH.Add(tmp.Tram, tmp);
                    }
                    //get DS dot thu hoach
                    Dictionary<int, int> soNgayHD = new Dictionary<int, int>();
                    soNgayHD.Add(11, 0);
                    soNgayHD.Add(12, 0);
                    soNgayHD.Add(1, 0);
                    soNgayHD.Add(2, 0);
                    soNgayHD.Add(3, 0);
                    soNgayHD.Add(4, 0);

                    List<DotTH> DSDotTH = new List<DotTH>();
                    qe = new QueryExpression("new_dotthuhoach");
                    qe.ColumnSet = new ColumnSet(new string[] { "new_tungay", "new_denngay", "new_songayhoatdong" });
                    qe.Criteria.AddCondition(new ConditionExpression("new_vuthuhoach", ConditionOperator.Equal, target.Id));
                    qe.AddOrder("new_tungay", OrderType.Ascending);
                    EntityCollection ent = service.RetrieveMultiple(qe);
                    foreach (Entity a in ent.Entities)
                    {
                        DotTH tmp = new DotTH();
                        tmp.dot = a.Id;
                        tmp.tu = ((DateTime)a["new_tungay"]).AddHours(7).Date;
                        tmp.den = ((DateTime)a["new_denngay"]).AddHours(7).Date;
                        tmp.songay = (int)a["new_songayhoatdong"];

                        int count = 1;
                        int month = tmp.tu.Month;
                        DateTime idx = tmp.tu;
                        while (idx < tmp.den)
                        {
                            if (idx.AddDays(1).Month != month || idx.AddDays(1) == tmp.den)
                            {
                                if (idx.AddDays(1) == tmp.den)
                                    count++;
                                soNgayHD[month] = soNgayHD[month] + count;
                                tmp.phanbo.Add(month, count);
                                count = 1;
                                idx = idx.AddDays(1);
                                month = idx.Month;
                            }
                            else {
                                idx = idx.AddDays(1);
                                count++;
                            }
                        }

                        DSDotTH.Add(tmp);
                    }

                    Dictionary<Guid, Dictionary<Guid, Thuadat>> ThuatheoTram = new Dictionary<Guid, Dictionary<Guid, Thuadat>>();
                    //get all thua dat
                    qe = new QueryExpression("new_thuadatcanhtac");
                    qe.ColumnSet = new ColumnSet(new string[] { "new_tram", "new_khachhang", "new_khachhangdoanhnghiep", "new_ngaythuhoachdukien", "new_sanluonguoc" });

                    LinkEntity l1 = new LinkEntity("new_thuadatcanhtac", "new_hopdongdautumia", "new_hopdongdautumia", "new_hopdongdautumiaid", JoinOperator.Inner);
                    l1.LinkCriteria.AddCondition(new ConditionExpression("new_vudautu", ConditionOperator.Equal, ((EntityReference)Vuthuhoach["new_vudautu"]).Id));
                    qe.LinkEntities.Add(l1);
                    qe.Criteria.AddCondition(new ConditionExpression("new_mucdichsanxuatmia", ConditionOperator.Equal, 100000001));
                    qe.Criteria.AddCondition(new ConditionExpression("statuscode", ConditionOperator.Equal, 100000000));

                    qe.AddOrder("new_tram", OrderType.Ascending);
                    qe.AddOrder("new_ngaythuhoachdukien", OrderType.Ascending);
                    qe.AddOrder("new_loaigocmia", OrderType.Descending);
                    qe.AddOrder("new_luugoc", OrderType.Descending);

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
                                Thuadat tmp = new Thuadat();
                                tmp.ThuaId = a.Id;
                                tmp.KhachHangId = (a.Contains("new_khachhang") ? ((EntityReference)a["new_khachhang"]).Id : ((EntityReference)a["new_khachhangdoanhnghiep"]).Id);
                                tmp.NgayTHDK = (DateTime)a["new_ngaythuhoachdukien"];
                                tmp.Sanluong = (a.Contains("new_sanluonguoc") ? (decimal)a["new_sanluonguoc"] : (decimal)0);

                                if (ThuatheoTram.ContainsKey(((EntityReference)a["new_tram"]).Id))
                                    ThuatheoTram[((EntityReference)a["new_tram"]).Id].Add(a.Id, tmp);
                                else {
                                    Dictionary<Guid, Thuadat> tmp2 = new Dictionary<Guid, Thuadat>();
                                    tmp2.Add(a.Id, tmp);
                                    ThuatheoTram.Add(((EntityReference)a["new_tram"]).Id, tmp2);
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

                    //bat dau phan
                    int thoigiantoida = (int)Vuthuhoach["new_thoigianth1thua"];

                    foreach (KeyValuePair<Guid, LichThang> a in DSTramTH)
                    {
                        foreach (DotTH b in DSDotTH)
                        {
                            if (!ThuatheoTram.ContainsKey(a.Key))
                                continue;
                            //tính quota cho đợt
                            int quotaDot = 0;
                            foreach (KeyValuePair<int, int> c in b.phanbo)
                            {
                                switch (c.Key)
                                {
                                    case 11:
                                        quotaDot += ((a.Value.Thang11 / soNgayHD[c.Key]) * c.Value);
                                        break;
                                    case 12:
                                        quotaDot += ((a.Value.Thang12 / soNgayHD[c.Key]) * c.Value);
                                        break;
                                    case 1:
                                        quotaDot += ((a.Value.Thang1 / soNgayHD[c.Key]) * c.Value);
                                        break;
                                    case 2:
                                        quotaDot += ((a.Value.Thang2 / soNgayHD[c.Key]) * c.Value);
                                        break;
                                    case 3:
                                        quotaDot += ((a.Value.Thang3 / soNgayHD[c.Key]) * c.Value);
                                        break;
                                    case 4:
                                        quotaDot += ((a.Value.Thang4 / soNgayHD[c.Key]) * c.Value);
                                        break;
                                }
                            }

                            //get all thua den han thu hoach theo tram a

                            Dictionary<Guid, int> QuotaKH = new Dictionary<Guid, int>();

                            int binhquanThua = (int)Vuthuhoach["new_khoiluongtoithieu"];
                            int binhquanKh = 0;
                            decimal tyleThua = (decimal)Vuthuhoach["new_quota_thuadat"] / 100;
                            decimal tyleKH = (decimal)Vuthuhoach["new_quotakhachhang"] / 100;

                            int countThua = 0;
                            int countKH = 0;
                            int tongQuota = 0;

                            List<Thuadat> DSThua = GetThuaDenHan(b.den, ThuatheoTram[a.Key], ref tongQuota, ref countThua, ref countKH, ref QuotaKH);
                            if (tongQuota < quotaDot)
                                DSThua = GetThuaDenHan(quotaDot, ThuatheoTram[a.Key], ref tongQuota, ref countThua, ref countKH, ref QuotaKH);

                            binhquanThua = ((quotaDot / countThua) > binhquanThua ? (quotaDot / countThua) : binhquanThua);
                            binhquanKh = (quotaDot / countKH);

                            foreach (Thuadat c in DSThua)
                            {
                                int bqThuamoi = binhquanThua;
                                int bqKHmoi = binhquanKh;

                                if (((b.tu - c.NgayTHDK).Days + 1) > thoigiantoida)
                                {
                                    bqThuamoi += decimal.ToInt32(binhquanThua * tyleThua);
                                    bqKHmoi += decimal.ToInt32(binhquanKh * tyleKH);
                                }

                                int sl = (c.Sanluong <= bqThuamoi ? decimal.ToInt32(c.Sanluong) : bqThuamoi);
                                if (QuotaKH[c.KhachHangId] + sl > bqKHmoi)
                                    sl = bqKHmoi - QuotaKH[c.KhachHangId];
                                if (sl <= 0)
                                    continue;

                                Entity lich10ngay = new Entity("new_lichthuhoach10ngay");
                                lich10ngay["new_vuthuhoach"] = target;
                                lich10ngay["new_dotthuhoach"] = new EntityReference("new_dotthuhoach", b.dot);
                                lich10ngay["new_ngaythuhoach"] = c.NgayTHDK;
                                lich10ngay["new_thuadatcanhtac"] = new EntityReference("new_thuadatcanhtac", c.ThuaId);
                                lich10ngay["new_tram"] = new EntityReference("businessunit", a.Key);
                                lich10ngay["new_sanluong"] = sl;

                                service.Create(lich10ngay);
                                QuotaKH[c.KhachHangId] += sl;

                                quotaDot -= sl;
                                ThuatheoTram[a.Key][c.ThuaId].Sanluong -= sl;
                                if (quotaDot <= 0)
                                    break;
                            }
                        }
                    }
                    context.OutputParameters["Return"] = "success";
                }
                else
                {
                    context.OutputParameters["Return"] = "Không tìm thấy Vụ thu hoạch, vui lòng save form trước khi chạy tính năng !";
                }
            }
            catch (Exception ex)
            {
                context.OutputParameters["Return"] = ex.Message + "-" + vl;
            }
        }

        public List<Thuadat> GetThuaDenHan(DateTime denngay, Dictionary<Guid, Thuadat> source, ref int tongquota, ref int countThua, ref int countKH, ref Dictionary<Guid, int> daPhan)
        {
            decimal tongsl = 0;
            List<Guid> dsThua = new List<Guid>();
            List<Guid> dsKH = new List<Guid>();
            daPhan = new Dictionary<Guid, int>();

            List<Thuadat> result = new List<Thuadat>();
            foreach (KeyValuePair<Guid, Thuadat> a in source)
                if (a.Value.NgayTHDK <= denngay && a.Value.Sanluong > 0)
                {
                    tongsl += a.Value.Sanluong;
                    dsKH.Add(a.Value.KhachHangId);
                    dsThua.Add(a.Value.ThuaId);
                    result.Add(a.Value);
                    if (!daPhan.ContainsKey(a.Value.KhachHangId))
                        daPhan.Add(a.Value.KhachHangId, 0);
                }
                else break;
            tongquota = decimal.ToInt32(tongsl);
            countThua = dsThua.Distinct().Count();
            countKH = dsKH.Distinct().Count();

            return result;
        }

        public List<Thuadat> GetThuaDenHan(decimal quota, Dictionary<Guid, Thuadat> source, ref int tongquota, ref int countThua, ref int countKH, ref Dictionary<Guid, int> daPhan)
        {
            decimal tongsl = 0;
            List<Guid> dsThua = new List<Guid>();
            List<Guid> dsKH = new List<Guid>();
            daPhan = new Dictionary<Guid, int>();

            List<Thuadat> result = new List<Thuadat>();
            foreach (KeyValuePair<Guid,Thuadat> a in source)
            {
                if (a.Value.Sanluong > 0)
                {
                    tongsl += a.Value.Sanluong;
                    dsKH.Add(a.Value.KhachHangId);
                    dsThua.Add(a.Value.ThuaId);
                    result.Add(a.Value);
                    if (!daPhan.ContainsKey(a.Value.KhachHangId))
                        daPhan.Add(a.Value.KhachHangId, 0);
                }
                if (tongsl >= quota)
                    break;
            }

            tongquota = decimal.ToInt32(tongsl);
            countThua = dsThua.Distinct().Count();
            countKH = dsKH.Distinct().Count();

            return result;
        }
    }
}
