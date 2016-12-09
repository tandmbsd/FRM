using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk.Messages;
using Microsoft.Crm.Sdk.Messages;

namespace Action_TinhLichThuHoach10Ngay
{
    public class Action_TinhLichThuHoach10Ngay : IPlugin
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
                    Dictionary<Guid, DoanTH> DSDoanTH = new Dictionary<Guid, DoanTH>();
                    Dictionary<Guid, TramTH> DSTramTH = new Dictionary<Guid, TramTH>(); //Quota thực tế từng trạm theo tháng
                    Dictionary<int, int> soNgayHD = new Dictionary<int, int>(); // {Thang, songay hoatdong} so ngay hoat dong tung thang

                    #region khoitao

                    QueryExpression qe = new QueryExpression();

                    if (Vuthuhoach != null)
                    {
                        //delete all lich thu hoach cu
                        qe = new QueryExpression("new_lichthuhoach10ngay");
                        qe.ColumnSet = new ColumnSet(new string[] { "new_lichthuhoach10ngayid" });
                        qe.Criteria.AddCondition(new ConditionExpression("new_vuthuhoach", ConditionOperator.Equal, target.Id));
                        EntityCollection en = service.RetrieveMultiple(qe);
                        foreach (Entity a in en.Entities)
                            service.Delete(a.LogicalName, a.Id);

                        //Get Doan thu hoach
                        qe = new QueryExpression("new_giaidoanthuhoach");
                        qe.ColumnSet = new ColumnSet(new string[] { "new_tungay", "new_denngay", "new_quotathuadat", "new_quotakhachhang", "new_thoigianth1thua", "new_toithieu1chuyen" });
                        qe.Criteria.AddCondition(new ConditionExpression("new_vuthuhoach", ConditionOperator.Equal, target.Id));
                        qe.Orders.Add(new OrderExpression("new_tungay", OrderType.Ascending));
                        EntityCollection doanth = service.RetrieveMultiple(qe);
                        foreach (Entity a in doanth.Entities)
                        {
                            DoanTH tmp = new DoanTH();
                            tmp.id = a.Id;
                            tmp.tu = ((DateTime)a["new_tungay"]).AddHours(7).Date;
                            tmp.den = ((DateTime)a["new_denngay"]).AddHours(7).Date;
                            tmp.quotathua = (decimal)a["new_quotathuadat"];
                            tmp.quotakh = (decimal)a["new_quotakhachhang"];
                            tmp.tgth1thua = (int)a["new_thoigianth1thua"];
                            tmp.kltoithieu1thua = (int)a["new_toithieu1chuyen"];
                            //=================
                            tmp.tltoithieu = new Dictionary<Guid, int>();
                            QueryExpression qex = new QueryExpression("new_trongluongtoithieu1chuyen");
                            qex.ColumnSet = new ColumnSet(new string[] { "new_tram", "new_khoiluongtoithieu" });
                            qex.Criteria.AddCondition(new ConditionExpression("new_giaidoanthuhoach", ConditionOperator.Equal, a.Id));

                            EntityCollection tl1chuyen = service.RetrieveMultiple(qex);
                            foreach (Entity l in tl1chuyen.Entities)
                                tmp.tltoithieu.Add(((EntityReference)l["new_tram"]).Id, (int)l["new_khoiluongtoithieu"]);
                            //=================
                            tmp.ttuutien = new List<Thutuuutien>();
                            QueryExpression qek = new QueryExpression("new_thutuuutienthuhoach");
                            qek.ColumnSet = new ColumnSet(new string[] { "new_thutu", "new_tieuchiuutien" });
                            qek.Criteria.AddCondition(new ConditionExpression("new_giaidoanthuhoach", ConditionOperator.Equal, a.Id));
                            qek.Orders.Add(new OrderExpression("new_thutu", OrderType.Ascending));

                            EntityCollection ttuutien = service.RetrieveMultiple(qek);
                            foreach (Entity l in ttuutien.Entities)
                                tmp.ttuutien.Add(new Thutuuutien((int)l["new_thutu"], ((OptionSetValue)l["new_tieuchiuutien"]).Value));
                            tmp.dotth = new Dictionary<Guid, DotTH>();

                            DSDoanTH.Add(tmp.id, tmp);
                        }

                        //get DS tram thu hoach
                        qe = new QueryExpression("new_lichthuhoachthang");
                        qe.ColumnSet = new ColumnSet(new string[] { "new_tram", "new_thang11", "new_thang12", "new_thang1", "new_thang2", "new_thang3", "new_thang4" });
                        qe.Criteria.AddCondition(new ConditionExpression("new_vuthuhoach", ConditionOperator.Equal, target.Id));
                        qe.Criteria.AddCondition(new ConditionExpression("new_loai", ConditionOperator.Equal, 100000001));

                        //qe.Criteria.AddCondition(new ConditionExpression("new_tram", ConditionOperator.Equal, new Guid("C9B9346E-2728-E611-80C1-9ABE942A7CB1")));

                        EntityCollection ens = service.RetrieveMultiple(qe);
                        foreach (Entity a in ens.Entities)
                        {
                            TramTH tmp = new TramTH();
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

                        soNgayHD.Add(11, 0);
                        soNgayHD.Add(12, 0);
                        soNgayHD.Add(1, 0);
                        soNgayHD.Add(2, 0);
                        soNgayHD.Add(3, 0);
                        soNgayHD.Add(4, 0);

                        qe = new QueryExpression("new_dotthuhoach");
                        qe.ColumnSet = new ColumnSet(new string[] { "new_giaidoanthuhoach", "new_tungay", "new_denngay", "new_songayhoatdong" });
                        qe.Criteria.AddCondition(new ConditionExpression("new_vuthuhoach", ConditionOperator.Equal, target.Id));
                        qe.AddOrder("new_tungay", OrderType.Ascending);
                        EntityCollection ent = service.RetrieveMultiple(qe);
                        List<DotTH> dsdottmp = new List<DotTH>();
                        foreach (Entity a in ent.Entities)
                        {
                            DotTH tmp = new DotTH();
                            tmp.id = a.Id;
                            tmp.tu = ((DateTime)a["new_tungay"]).AddHours(7).Date;
                            tmp.den = ((DateTime)a["new_denngay"]).AddHours(7).Date;

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


                            if (DSDoanTH.ContainsKey(((EntityReference)a["new_giaidoanthuhoach"]).Id))
                                DSDoanTH[((EntityReference)a["new_giaidoanthuhoach"]).Id].dotth.Add(tmp.id, tmp);
                            
                            dsdottmp.Add(tmp);
                        }

                        foreach (DotTH tmp in dsdottmp)
                        {
                            foreach (KeyValuePair<Guid, TramTH> l in DSTramTH)
                            {
                                int quotaDot = 0;
                                foreach (KeyValuePair<int, int> c in tmp.phanbo)
                                {
                                    switch (c.Key)
                                    {
                                        case 11:
                                            quotaDot += l.Value.Thang11 * c.Value / soNgayHD[c.Key];
                                            break;
                                        case 12:
                                            quotaDot += l.Value.Thang12 * c.Value / soNgayHD[c.Key];
                                            break;
                                        case 1:
                                            quotaDot += l.Value.Thang1 * c.Value / soNgayHD[c.Key];
                                            break;
                                        case 2:
                                            quotaDot += l.Value.Thang2 * c.Value / soNgayHD[c.Key];
                                            break;
                                        case 3:
                                            quotaDot += l.Value.Thang3 * c.Value / soNgayHD[c.Key];
                                            break;
                                        case 4:
                                            quotaDot += l.Value.Thang4 * c.Value / soNgayHD[c.Key];
                                            break;
                                    }
                                }
                                l.Value.quotadot.Add(tmp.id, quotaDot);
                            }
                        }
                    }
                    #endregion

                    //Get cong no KH
                    Dictionary<Guid, Decimal> DSNoKH = new Dictionary<Guid, decimal>();
                    string fetch = @"<fetch distinct='true' mapping='logical' aggregate='true'>" +
                      "<entity name='new_phanbodautu'>" +
                       " <attribute name='new_khachhang' alias='new_khachhang' groupby='true'/>" +
                       " <attribute name='new_khachhangdoanhnghiep' alias='new_khachhangdoanhnghiep' groupby='true'/>" +
                       " <attribute name='new_conlai' alias='new_conlai' aggregate='sum'/>" +
                       " <link-entity name='new_vudautu' to='new_vuthanhtoan'>" +
                        "  <filter type='and'>" +
                         "   <condition attribute='new_namtaichinhh' operator='lt' value='" + nam + "' />" +
                         " </filter>" +
                       " </link-entity>" +
                       " <filter type='and'>" +
                       "   <condition attribute='new_conlai' operator='gt' value='0' />" +
                       " </filter>" +
                      "</entity>" +
                    "</fetch>";

                    FetchExpression FetchQuery = new FetchExpression(fetch);
                    EntityCollection Results = service.RetrieveMultiple(FetchQuery);

                    foreach (Entity a in Results.Entities)
                    {
                        if (!a.Contains("new_khachhang") && !a.Contains("new_khachhangdoanhnghiep"))
                            continue;

                        decimal no = (a.Contains("new_conlai") ? ((Money)((AliasedValue)a["new_conlai"]).Value).Value : (decimal)0);
                        if (a.Contains("new_khachhang"))
                            DSNoKH.Add(((EntityReference)((AliasedValue)a["new_khachhang"]).Value).Id, no);
                        else
                            DSNoKH.Add(((EntityReference)((AliasedValue)a["new_khachhangdoanhnghiep"])
                                .Value).Id, no);
                    }

                    //get all thua dat
                    #region Sap thua theo tram
                    qe = new QueryExpression("new_thuadatcanhtac");
                    qe.ColumnSet = new ColumnSet(new string[] { "new_ngaytrong", "new_tram", "new_khachhang", "new_khachhangdoanhnghiep", "new_ngaythuhoachdukien", "new_ngaydukienthuhoachsom", "new_sanluonguoc", "new_loaigocmia", "new_luugoc", "new_ngayktbonphan", "new_tuoimia" });

                    LinkEntity l1 = new LinkEntity("new_thuadatcanhtac", "new_hopdongdautumia", "new_hopdongdautumia", "new_hopdongdautumiaid", JoinOperator.Inner);
                    l1.LinkCriteria.AddCondition(new ConditionExpression("new_vudautu", ConditionOperator.Equal, ((EntityReference)Vuthuhoach["new_vudautu"]).Id));
                    qe.LinkEntities.Add(l1);

                    LinkEntity l2 = new LinkEntity("new_thuadatcanhtac", "new_chitietbbthuhoachsom", "new_thuadatcanhtacid", "new_chitiethddtmia", JoinOperator.LeftOuter);
                    l2.LinkCriteria.AddCondition(new ConditionExpression("statuscode", ConditionOperator.Equal, 100000000));
                    qe.LinkEntities.Add(l2);

                    qe.LinkEntities[1].Columns.AddColumns("new_ngaydukienthuhoach", "new_lydothuhoachsom", "new_chitietbbthuhoachsomid");
                    qe.LinkEntities[1].EntityAlias = "c";
                    qe.Criteria.AddCondition(new ConditionExpression("new_sanluonguoc", ConditionOperator.GreaterThan, (decimal)0));
                    qe.Criteria.AddCondition(new ConditionExpression("statuscode", ConditionOperator.Equal, 100000000));
                    qe.Criteria.AddCondition(new ConditionExpression("new_ngaytrong", ConditionOperator.NotNull));
                    qe.Criteria.AddCondition(new ConditionExpression("new_dientichconlai", ConditionOperator.GreaterThan, (decimal)0));

                    //qe.Criteria.AddCondition(new ConditionExpression("new_tram", ConditionOperator.Equal, new Guid("C9B9346E-2728-E611-80C1-9ABE942A7CB1")));

                    //qe.AddOrder("new_tram", OrderType.Ascending);
                    //qe.AddOrder("new_ngaythuhoachdukien", OrderType.Ascending);
                    //qe.AddOrder("new_loaigocmia", OrderType.Descending);
                    //qe.AddOrder("new_luugoc", OrderType.Descending);

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
                                Guid KHId = (a.Contains("new_khachhang") ? ((EntityReference)a["new_khachhang"]).Id : ((EntityReference)a["new_khachhangdoanhnghiep"]).Id);
                                Thuadat tmp = new Thuadat()
                                {
                                    ThuaId = a.Id,
                                    KhachHangId = KHId,
                                    NgayTHDK = ((a.Contains("new_ngaydukienthuhoachsom") ? (DateTime)a["c.new_ngaydukienthuhoachsom"] : (a.Contains("c.new_ngaydukienthuhoach") ? (DateTime)a["c.new_ngaydukienthuhoach"] : (DateTime)a["new_ngaythuhoachdukien"]))).AddHours(7).Date,
                                    Loaigocmia = (a.Contains("new_loaigocmia") ? ((OptionSetValue)a["new_loaigocmia"]).Value : 100000000),
                                    NoQuaHan = DSNoKH.ContainsKey(KHId) ? DSNoKH[KHId] : 0,
                                    Sanluong = (a.Contains("new_sanluonguoc") ? (decimal)a["new_sanluonguoc"] : (decimal)0),
                                    Ngaytrong = ((a.Contains("new_ngaytrong") ? ((DateTime)a["new_ngaytrong"]).AddHours(7) : new DateTime(1900, 1, 1))).AddHours(7).Date
                                };

                                if (a.Contains("new_luugoc"))
                                    tmp.Luugoc = ((OptionSetValue)a["new_luugoc"]).Value;

                                if (a.Contains("new_ngayktbonphan"))
                                    tmp.NgayKTBP = ((DateTime)a["new_ngayktbonphan"]).AddHours(7);

                                if (a.Contains("new_tuoimia"))
                                    tmp.Tuoi = (bool)a["new_tuoimia"];

                                if (a.Contains("c.new_lydothuhoachsom"))
                                {
                                    tmp.Lydothsom = ((EntityReference)a["c.new_lydothuhoachsom"]).Id;
                                    tmp.CTBBThuhoachsom = ((EntityReference)a["c.new_chitietbbthuhoachsomid"]).Id;
                                }

                                if (!DSTramTH.ContainsKey(((EntityReference)a["new_tram"]).Id))
                                    continue;
                                else
                                    DSTramTH[((EntityReference)a["new_tram"]).Id].dsthua.Add(tmp);
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
                    #endregion

                    //bat dau phan

                    Guid preGD = Guid.Empty;

                    foreach (KeyValuePair<Guid, TramTH> a in DSTramTH)
                    {
                        //if (a.Key == new Guid("AFB9346E-2728-E611-80C1-9ABE942A7CB1"))
                        //{
                        //    Console.WriteLine("sadsad");
                        //}

                        foreach (KeyValuePair<Guid, DoanTH> b in DSDoanTH)
                        {
                            int sltoithieu = b.Value.tltoithieu.ContainsKey(a.Key) ? b.Value.tltoithieu[a.Key] : b.Value.kltoithieu1thua;
                            int binhquanThua = b.Value.tltoithieu.ContainsKey(a.Key) ? b.Value.tltoithieu[a.Key] : b.Value.kltoithieu1thua;
                            decimal tyleThua = b.Value.quotathua / 100;
                            decimal tyleKH = b.Value.quotakh / 100;
                            int thoigiantoida = b.Value.tgth1thua;

                            //reorder Thuadat
                            if (preGD == Guid.Empty || b.Key != preGD)
                            {
                                IOrderedEnumerable<Thuadat> listsoft = Enumerable.Empty<Thuadat>().OrderBy(x => 1);

                                int start = 0;
                                foreach (Thutuuutien t in b.Value.ttuutien)
                                {
                                    switch (t.lydo)
                                    {
                                        case 100000000: //Tuoi mia
                                            if (start == 0)
                                            {
                                                listsoft = a.Value.dsthua.OrderBy(o => o.NgayTHDK);
                                                start++;
                                            }
                                            else listsoft = listsoft.ThenBy(o => o.NgayTHDK);
                                            break;

                                        case 100000001:
                                            if (start == 0)
                                            {
                                                listsoft = a.Value.dsthua.OrderByDescending(o => o.Loaigocmia).ThenByDescending(o => o.Luugoc);
                                                start++;
                                            }
                                            else listsoft = listsoft.ThenByDescending(o => o.Loaigocmia).ThenByDescending(o => o.Luugoc);
                                            break;

                                        case 100000002:
                                            if (start == 0)
                                            {
                                                listsoft = a.Value.dsthua.OrderBy(o => o.NgayKTBP);
                                                start++;
                                            }
                                            else listsoft = listsoft.ThenBy(o => o.NgayKTBP);
                                            break;

                                        case 100000003:
                                            if (start == 0)
                                            {
                                                listsoft = a.Value.dsthua.OrderByDescending(o => o.Tuoi);
                                                start++;
                                            }
                                            else listsoft = listsoft.ThenByDescending(o => o.Tuoi);
                                            break;

                                        default:
                                            if (start == 0)
                                            {
                                                listsoft = a.Value.dsthua.OrderByDescending(o => o.NoQuaHan);
                                                start++;
                                            }
                                            else listsoft = listsoft.ThenByDescending(o => o.NoQuaHan);
                                            break;
                                    }
                                }
                                a.Value.dsthua = listsoft.ToList();
                                preGD = b.Key;
                            }

                            int tottal = 0;
                            foreach (KeyValuePair<Guid, DotTH> c in b.Value.dotth)
                            {
                                int quotaDot = a.Value.quotadot[c.Key];
                                if (quotaDot > 0)
                                {
                                    Dictionary<Guid, int> QuotaKH = new Dictionary<Guid, int>();

                                    int countThua = 0;
                                    int countKH = 0;
                                    int tongQuota = 0;
                                    int bqVuot = 0;
                                    // int binhquanKh = 0;

                                    List<Thuadat> DSThua = GetThuaDenHan(binhquanThua, c.Value.den, a.Value.dsthua, ref tongQuota, ref countThua, ref countKH, ref QuotaKH, ref bqVuot);
                                    if (tongQuota < quotaDot)
                                        DSThua = GetThuaDenHan(binhquanThua, quotaDot, a.Value.dsthua, ref tongQuota, ref countThua, ref countKH, ref QuotaKH, ref bqVuot);
                                    if (DSThua.Count > 0)
                                    {
                                        binhquanThua = bqVuot;
                                        // binhquanKh = (quotaDot / countKH);

                                        int bqThuamoi = binhquanThua + decimal.ToInt32(binhquanThua * tyleThua);
                                        //int bqKHmoi = binhquanKh + decimal.ToInt32(binhquanKh * tyleKH);

                                        foreach (Thuadat d in DSThua)
                                        {
                                            int maxThua = binhquanThua;
                                            //int maxKH = binhquanKh;

                                            if (((c.Value.tu - d.NgayTHDK).Days + 1) > thoigiantoida)
                                            {
                                                maxThua = bqThuamoi;
                                                // maxKH = bqKHmoi;
                                            }

                                            int sl = (d.Sanluong <= maxThua ? decimal.ToInt32(d.Sanluong) : maxThua);
                                            //if (QuotaKH[d.KhachHangId] + sl > maxKH)
                                            //    sl = maxKH - QuotaKH[d.KhachHangId];
                                            if (sl <= 0)
                                                continue;

                                            if (quotaDot < sl)
                                                sl = quotaDot;

                                            if (d.Sanluong - sl > 0 && d.Sanluong - sl < sltoithieu)
                                                sl = decimal.ToInt32(d.Sanluong);

                                            Entity lich10ngay = new Entity("new_lichthuhoach10ngay");
                                            lich10ngay["new_vuthuhoach"] = target;
                                            lich10ngay["new_dotthuhoach"] = new EntityReference("new_dotthuhoach", c.Key);
                                            lich10ngay["new_ngaythuhoach"] = d.NgayTHDK;
                                            lich10ngay["new_thuadatcanhtac"] = new EntityReference("new_thuadatcanhtac", d.ThuaId);
                                            lich10ngay["new_tram"] = new EntityReference("businessunit", a.Key);
                                            lich10ngay["new_sanluong"] = sl;
                                            if (d.Lydothsom != Guid.Empty)
                                            {
                                                lich10ngay["new_lydothuhoachsom"] = new EntityReference("new_lydo", d.Lydothsom);
                                                lich10ngay["new_chitietbbthuhoachsom"] = new EntityReference("new_chitietbbthuhoachsom", d.CTBBThuhoachsom);
                                            }
                                            lich10ngay["new_tuoimia"] = (decimal)(c.Value.tu - d.Ngaytrong).TotalDays / (decimal)30;

                                            tottal += sl;
                                            service.Create(lich10ngay);
                                            QuotaKH[d.KhachHangId] += sl;

                                            quotaDot -= sl;

                                            a.Value.dsthua.Find(o => o.ThuaId == d.ThuaId).Sanluong -= sl;
                                            if (quotaDot <= 0)
                                                break;
                                        }
                                    }
                                }
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
        public List<Thuadat> GetThuaDenHan(int bqThua, DateTime denngay, List<Thuadat> source, ref int tongquota, ref int countThua, ref int countKH, ref Dictionary<Guid, int> daPhan, ref int bqVuot)
        {
            decimal tongsl = 0;
            decimal tonglonbq = 0;
            int countlonbq = 0;
            List<Guid> dsThua = new List<Guid>();
            List<Guid> dsKH = new List<Guid>();
            daPhan = new Dictionary<Guid, int>();

            List<Thuadat> result = new List<Thuadat>();
            foreach (Thuadat a in source)
                if (a.NgayTHDK <= denngay && a.Sanluong > 0)
                {
                    tongsl += a.Sanluong;
                    dsKH.Add(a.KhachHangId);
                    dsThua.Add(a.ThuaId);
                    result.Add(a);
                    if (!daPhan.ContainsKey(a.KhachHangId))
                        daPhan.Add(a.KhachHangId, 0);
                    if (a.Sanluong > bqThua)
                    {
                        tonglonbq += a.Sanluong;
                        countlonbq++;
                    }
                }
                else continue;
            tongquota = decimal.ToInt32(tongsl);
            countThua = dsThua.Distinct().Count();
            countKH = dsKH.Distinct().Count();
            if (tonglonbq > 0)
                bqVuot = decimal.ToInt32(tonglonbq / countlonbq);
            else bqVuot = bqThua;
            return result;
        }

        public List<Thuadat> GetThuaDenHan(int bqThua, decimal quota, List<Thuadat> source, ref int tongquota, ref int countThua, ref int countKH, ref Dictionary<Guid, int> daPhan, ref int bqVuot)
        {
            decimal tongsl = 0;
            decimal tonglonbq = 0;
            int countlonbq = 0;
            List<Guid> dsThua = new List<Guid>();
            List<Guid> dsKH = new List<Guid>();
            daPhan = new Dictionary<Guid, int>();

            List<Thuadat> result = new List<Thuadat>();
            foreach (Thuadat a in source)
            {
                if (a.Sanluong > 0)
                {
                    tongsl += a.Sanluong;
                    dsKH.Add(a.KhachHangId);
                    dsThua.Add(a.ThuaId);
                    result.Add(a);
                    if (!daPhan.ContainsKey(a.KhachHangId))
                        daPhan.Add(a.KhachHangId, 0);
                    if (a.Sanluong > bqThua)
                    {
                        tonglonbq += a.Sanluong;
                        countlonbq++;
                    }
                }

                if (tongsl >= quota)
                    break;
            }

            tongquota = decimal.ToInt32(tongsl);
            countThua = dsThua.Distinct().Count();
            countKH = dsKH.Distinct().Count();
            if (tonglonbq > 0)
                bqVuot = decimal.ToInt32(tonglonbq / countlonbq);
            else bqVuot = bqThua;

            return result;
        }
    }
}
