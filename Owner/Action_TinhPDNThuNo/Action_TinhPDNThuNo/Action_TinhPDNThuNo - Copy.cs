using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Action_TinhPDNThuNo
{
    public class Action_TinhPDNThuNo : IPlugin
    {
        IOrganizationService service = null;
        IOrganizationServiceFactory factory = null;
        public ITracingService trace;
        string hinhthuctra = null;
        void IPlugin.Execute(IServiceProvider serviceProvider)
        {
            IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
            service = factory.CreateOrganizationService(context.UserId);
            trace = (ITracingService)serviceProvider.GetService(typeof(ITracingService));

            Guid entityID = new Guid((string)context.InputParameters["ObjectId"]);
            string entityName = (string)context.InputParameters["Entity"];
            string customerType = (string)context.InputParameters["CustomerType"];
            string customerID = (string)context.InputParameters["CustomerId"];
            hinhthuctra = (string)context.InputParameters["HinhThucTra"];
            string sotienStr = (string)context.InputParameters["SoTien"];
            decimal sotien = 0;

            if (Decimal.TryParse(sotienStr, out sotien) == false)
                throw new Exception("Số tiền nhập vào không hợp lệ");

            if (hinhthuctra == null)
                throw new Exception("Thiếu tham số hình thức trả");

            Entity target = service.Retrieve(entityName, entityID,
                new ColumnSet(new string[] { entityName + "id", "new_khachhang", "new_khachhangdoanhnghiep",
                    "new_hinhthuctra", "new_vudautu","new_ngaythu","new_thunogoc","new_tongtienthu"}));
            phieudenghithunoID = target.Id;

            if (!target.Contains("new_vudautu"))
                throw new Exception("Phiếu đề nghị thu nợ không có vụ đầu tư ");

            TinhLai(target);
        }

        private decimal tragoc = 0;
        private decimal tongtien = 0;
        private decimal tonglai = 0;
        private int flag = 0;
        private DateTime ngaythu;
        private Guid phieudenghithunoID;
        private decimal TinhLai(Entity target)
        {
            if (!target.Contains("new_vudautu"))
                throw new Exception("Phiếu đề nghị thu nợ không có vụ đầu tư");

            Entity vudautu = service.Retrieve("new_vudautu", ((EntityReference)target["new_vudautu"]).Id,
                new ColumnSet(new string[] { "new_cachtinhlai", "new_thutuuutien", "new_name", "new_namtaichinhh" }));
            List<Phanbodautu> phanbodautus = OrderPBDT(target, vudautu);

            if (phanbodautus == null || phanbodautus.Count == 0)
                throw new Exception("Khách hàng đã hết nợ");

            if (!vudautu.Contains("new_cachtinhlai"))
                throw new Exception("Vụ đầu tư không có cách tính lãi");

            int cachtinhlai = ((OptionSetValue)vudautu["new_cachtinhlai"]).Value;

            if (!target.Contains("new_ngaythu"))
                throw new Exception("Phiếu đề nghị thu nợ không có ngày thu");

            ngaythu = (DateTime)target["new_ngaythu"];

            if (hinhthuctra == "100000000") // tra goc
            {
                tragoc = target.Contains("new_thunogoc") ? ((Money)target["new_thunogoc"]).Value : new decimal(0);
                switch (cachtinhlai)
                {
                    case 100000000: // lai tren tien thu
                        {
                            Tragoc_Laitrentienthu(phanbodautus, tragoc, target);
                            break;
                        }
                    case 100000001: // lai tren tong tien
                        {
                            Tragoc_Laitrentongtien(phanbodautus, tragoc, target);
                            break;
                        }
                }
            }
            else if (hinhthuctra == "100000001") // tong tien
            {
                tongtien = target.Contains("new_tongtienthu") ? ((Money)target["new_tongtienthu"]).Value : new decimal(0);
                switch (cachtinhlai)
                {
                    case 100000000: // lai tren tien thu
                        {
                            Tongtien_Laitrentienthu(phanbodautus, tongtien, target);
                            break;
                        }
                    case 100000001: // lai tren tong tien
                        {
                            Tongtien_Laitrentongtien(phanbodautus, tongtien, target);
                            break;
                        }
                }
            }

            return 0;
        }

        private void ClearAllPhieutinhlai()
        {
            EntityCollection lstPhieutinhlai = RetrieveNNRecord(service, "new_phieutinhlai", "new_phieudenghithuno",
                "new_new_phieutinhlai_new_phieudenghithuno", new ColumnSet(true), "new_phieudenghithunoid", phieudenghithunoID);

            EntityReferenceCollection t = new EntityReferenceCollection();

            foreach (Entity a in lstPhieutinhlai.Entities)
            {
                t.Add(a.ToEntityReference());
            }

            service.Disassociate("new_phieudenghithuno", phieudenghithunoID, new Relationship("new_new_phieutinhlai_new_phieudenghithuno"),
                t);

            foreach (Entity en in lstPhieutinhlai.Entities)
                service.Delete(en.LogicalName, en.Id);

        }

        private List<Phanbodautu> OrderPBDT(Entity target, Entity vudautu)
        {
            if (!vudautu.Contains("new_thutuuutien"))
                throw new Exception("Vụ đầu tư không có thứ tự ưu tiên");
            Entity vudautuhientai = GetVDTHienTai();

            QueryExpression q = new QueryExpression("new_phanbodautu");
            q.ColumnSet = new ColumnSet(true);
            q.Criteria = new FilterExpression();

            if (target.Contains("new_khachhang"))
                q.Criteria.AddCondition(new ConditionExpression("new_khachhang", ConditionOperator.Equal,
                    ((EntityReference)target["new_khachhang"]).Id));

            else if (target.Contains("new_khachhangdoanhnghiep"))
                q.Criteria.AddCondition(new ConditionExpression("new_khachhangdoanhnghiep", ConditionOperator.Equal,
                    ((EntityReference)target["new_khachhangdoanhnghiep"]).Id));
            FilterExpression f1 = new FilterExpression(LogicalOperator.Or);
            f1.AddCondition(new ConditionExpression("new_conlai", ConditionOperator.Null));
            f1.AddCondition(new ConditionExpression("new_conlai", ConditionOperator.GreaterThan, decimal.Zero));
            //FilterExpression f2 = new FilterExpression(LogicalOperator.And);
            //f2.AddCondition(new ConditionExpression("new_loaidautu", ConditionOperator.Equal, 100000000));
            q.Criteria.AddFilter(f1);
            //q.Criteria.AddFilter(f2);
            q.AddOrder("new_ngayphatsinh", OrderType.Ascending);
            q.Criteria.AddCondition("statecode", ConditionOperator.Equal, 0);

            EntityCollection entc = service.RetrieveMultiple(q);

            List<Entity> lstChitietthutuuutien = RetrieveThutuUutien(service, "new_chitietthutuuutien",
                new ColumnSet(new string[] { "new_uutien" }), "new_thutuuutien", ((EntityReference)vudautu["new_thutuuutien"]).Id);

            List<Phanbodautu> lstPBDTResult = new List<Phanbodautu>();
            //List<Phanbodautu> lstPBDT_Ngoaivu = new List<Phanbodautu>();
            //List<Phanbodautu> lstPBDT_Tamung = new List<Phanbodautu>();

            foreach (Entity en in entc.Entities)
            {
                if (!en.Contains("new_vuthanhtoan"))
                    throw new Exception("Phân bổ đầu tư không có vụ thanh toán ");

                int loaihopdong = en.Contains("new_loaihopdong") ? ((OptionSetValue)en["new_loaihopdong"]).Value : -1;
                int mucdichdautu = en.Contains("new_mucdichdautu") ? ((OptionSetValue)en["new_mucdichdautu"]).Value : -1;

                Entity vudautuPBDT = service.Retrieve("new_vudautu", ((EntityReference)en["new_vuthanhtoan"]).Id,
                    new ColumnSet(new string[] { "new_namtaichinhh", "new_name" }));

                foreach (Entity t in lstChitietthutuuutien)
                {
                    int uutien = ((OptionSetValue)t["new_uutien"]).Value;

                    switch (uutien)
                    {
                        case 100000000: // tam ung
                            if (mucdichdautu == 100000004) // tam ungs
                                lstPBDTResult.Add(CreatePBDT(en));
                            break;
                        case 100000001: // dau tu trong mia trog hạn
                            if (CompareVudautu(vudautuPBDT, vudautu) == 0 && (mucdichdautu == 100000000 ||
                            mucdichdautu == 100000001 || mucdichdautu == 100000002 || mucdichdautu == 100000003))
                            {
                                lstPBDTResult.Add(CreatePBDT(en));
                            }
                            break;
                        case 100000002: // dau tu trong mia qua han
                            if (CompareVudautu(vudautuPBDT, vudautu) == 1 && (mucdichdautu == 100000000 ||
                            mucdichdautu == 100000001 || mucdichdautu == 100000002 || mucdichdautu == 100000003))
                            {
                                lstPBDTResult.Add(CreatePBDT(en));
                            }
                            break;
                        case 100000003: // dau tu thue dat trong han
                            if (CompareVudautu(vudautuPBDT, vudautu) == 0 && mucdichdautu == 100000005)
                            {
                                lstPBDTResult.Add(CreatePBDT(en));
                            }
                            break;
                        case 100000004: // dau tu thue dat qua han
                            if (CompareVudautu(vudautuPBDT, vudautu) == 1 && mucdichdautu == 100000005)
                            {
                                lstPBDTResult.Add(CreatePBDT(en));
                            }
                            break;
                        case 100000005: // dau tu mmtb trong hạn
                            if (CompareVudautu(vudautuPBDT, vudautu) == 0 && mucdichdautu == 100000006)
                            {
                                lstPBDTResult.Add(CreatePBDT(en));
                            }
                            break;
                        case 100000006: // dau tu mmtb quá hạn
                            if (CompareVudautu(vudautuPBDT, vudautu) == 1 && mucdichdautu == 100000006)
                            {
                                lstPBDTResult.Add(CreatePBDT(en));
                            }
                            break;
                        case 100000007: // dau tu ha tang trong han
                            if (CompareVudautu(vudautuPBDT, vudautu) == 0 && mucdichdautu == 100000007)
                            {
                                lstPBDTResult.Add(CreatePBDT(en));
                            }
                            break;
                        case 100000008: // dau tu ha tang quá hạn
                            if (CompareVudautu(vudautuPBDT, vudautu) == 1 && mucdichdautu == 100000007)
                            {
                                lstPBDTResult.Add(CreatePBDT(en));
                            }
                            break;
                    }
                }
            }

            //if (lstPBDT_Trongvu != null && lstPBDT_Trongvu.Count != 0)
            //{
            //    lstPBDT_Trongvu = lstPBDT_Trongvu.OrderBy(p => p.namtaichinhvuthanhtoan).ThenBy(p => p.ngayphatsinh).ToList();

            //    if (lstPBDT_Ngoaivu != null && lstPBDT_Ngoaivu.Count != 0)
            //    {
            //        lstPBDT_Ngoaivu = lstPBDT_Ngoaivu.OrderBy(p => p.namtaichinhvuthanhtoan).ThenBy(p => p.ngayphatsinh).ToList();
            //        lstPBDT_Trongvu.AddRange(lstPBDT_Ngoaivu);
            //    }

            //    return lstPBDT_Trongvu;
            //}
            //else
            //{
            //    lstPBDT_Ngoaivu = lstPBDT_Ngoaivu.OrderBy(p => p.namtaichinhvuthanhtoan).ThenBy(p => p.ngayphatsinh).ToList();
            //    return lstPBDT_Ngoaivu;
            //}
            return lstPBDTResult;
        }

        List<Entity> RetrieveThutuUutien(IOrganizationService crmservices, string entity, ColumnSet column, string condition, object value)
        {
            QueryExpression q = new QueryExpression(entity);
            q.ColumnSet = column;
            q.Criteria = new FilterExpression();
            q.Criteria.AddCondition(new ConditionExpression(condition, ConditionOperator.Equal, value));
            q.Orders.Add(new OrderExpression("new_name", OrderType.Ascending));
            EntityCollection entc = service.RetrieveMultiple(q);

            return entc.Entities.ToList<Entity>();
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

        private Phanbodautu CreatePBDT(Entity en) // en : pbdt
        {
            Phanbodautu pbdt = new Phanbodautu();
            pbdt.ID = en.Id;
            pbdt.sotien = en.Contains("new_conlai") ? ((Money)en["new_conlai"]).Value : new decimal(0);

            if (!en.Contains("new_ngayphatsinh"))
                throw new Exception(en["new_name"].ToString() + " không có ngày phát sinh");

            if (!en.Contains("new_vuthanhtoan"))
                throw new Exception(en["new_name"].ToString() + " không có vụ thanh toán");

            if (!en.Contains("new_vudautu"))
                throw new Exception(en["new_name"].ToString() + " không có vụ đầu tư");

            Entity vuthanhtoan = service.Retrieve("new_vudautu", ((EntityReference)en["new_vuthanhtoan"]).Id,
                new ColumnSet(new string[] { "new_namtaichinhh", "new_name" }));

            if (!vuthanhtoan.Contains("new_namtaichinhh"))
                throw new Exception(vuthanhtoan["new_name"].ToString() + " không có năm tài chính");

            if (!en.Contains("new_loaihopdong"))
                throw new Exception(en["new_name"].ToString() + " không có loại hợp đồng");

            pbdt.loaihopdong = ((OptionSetValue)en["new_loaihopdong"]).Value;
            pbdt.masohphieuphanbo = en["new_maphieuphanbo"].ToString();
            pbdt.namtaichinhvuthanhtoan = Int32.Parse((string)vuthanhtoan["new_namtaichinhh"]);
            pbdt.ngaytinhlaisaucung = en.Contains("new_ngaytinhlaisaucung") ? (DateTime)en["new_ngaytinhlaisaucung"] : (DateTime)en["new_ngayphatsinh"];
            pbdt.ngayphatsinh = (DateTime)en["new_ngayphatsinh"];
            pbdt.nolai = en.Contains("new_nolai") ? ((Money)en["new_nolai"]).Value : new decimal(0);
            pbdt.vudautu = (EntityReference)en["new_vudautu"];
            pbdt.loailaisuat = en.Contains("new_loailaisuat") ? ((OptionSetValue)en["new_loailaisuat"]).Value : 0;

            if (!en.Contains("new_laisuat"))
                throw new Exception(en["new_name"].ToString() + " thiếu lãi suất cố định");

            pbdt.laisuat = (decimal)en["new_laisuat"];


            return pbdt;
        }

        private int CompareVudautu(Entity vdt1, Entity vdt2)
        {
            if (!vdt1.Contains("new_namtaichinhh"))
                throw new Exception(vdt1["new_name"].ToString() + " không có năm tài chính ");

            if (!vdt2.Contains("new_namtaichinhh"))
                throw new Exception(vdt2["new_name"].ToString() + " không có năm tài chính ");

            int namtaichinh1 = Int32.Parse((string)vdt1["new_namtaichinhh"]);
            int namtaichinh2 = Int32.Parse((string)vdt2["new_namtaichinhh"]);
            //throw new Exception(namtaichinh1.ToString() + namtaichinh2.ToString());
            int result = -1;
            if (namtaichinh1 < namtaichinh2)
                result = 1;
            else if (namtaichinh1 == namtaichinh2)
                result = 0;

            return result;
        }

        private Entity GetVDTHienTai()
        {
            QueryExpression q = new QueryExpression("new_vudautu");
            q.ColumnSet = new ColumnSet(true);
            q.Criteria = new FilterExpression();
            q.Criteria.AddCondition(new ConditionExpression("new_danghoatdong", ConditionOperator.Equal, true));
            q.AddOrder("new_namtaichinhh", OrderType.Descending);
            EntityCollection entc = service.RetrieveMultiple(q);

            return entc.Entities.ToList<Entity>().FirstOrDefault();
        }

        private decimal Tinhlaitungphieuphanbo(Phanbodautu pbdt, decimal sotien, DateTime begin, DateTime end, int flag)
        {
            decimal result = 0;
            decimal laibandau = 0;
            List<object> ls = new List<object>();

            if (pbdt.loailaisuat == 100000000) // co dinh
            {
                result = (decimal)end.Date.Subtract(begin.Date).TotalDays * (pbdt.laisuat / 3000) * sotien;

                if (flag == 1) // hinh thuc tra goc
                    CreatePhieutinhlai(sotien, begin, end, pbdt.laisuat, result, pbdt,
                        (int)end.Date.Subtract(begin.Date).TotalDays, 0, sotien, 0);
                else if (flag == 2) // hinh thuc tra tong tien
                {
                    if (gocthat > 0)
                        CreatePhieutinhlai(sotien, begin, end, pbdt.laisuat, result, pbdt,
                        (int)end.Date.Subtract(begin.Date).TotalDays, nolai, sotien, pbdt.sotien - sotien);
                }
            }
            else if (pbdt.loailaisuat == 100000001) // thay doi
            {
                QueryExpression qbangLai = new QueryExpression("new_banglaisuatthaydoi");
                qbangLai.ColumnSet = new ColumnSet(new string[] { "new_name", "new_ngayapdung", "new_phantramlaisuat", "new_ma" });
                qbangLai.Criteria = new FilterExpression(LogicalOperator.And);
                //qbangLai.Criteria.AddCondition(new ConditionExpression("new_chinhsachdautu", ConditionOperator.Equal,
                //    ((EntityReference)thuacanhtac["new_chinhsachdautu"]).Id));
                qbangLai.Criteria.AddCondition(new ConditionExpression("statecode", ConditionOperator.Equal, 0));
                qbangLai.Criteria.AddCondition(new ConditionExpression("new_vudautuapdung", ConditionOperator.Equal, pbdt.vudautu.Id));

                if (pbdt.loaihopdong == 100000000) // hop dong mia
                {
                    FilterExpression hopdongmia = new FilterExpression(LogicalOperator.Or);
                    hopdongmia.AddCondition(new ConditionExpression("new_mucdichdautu", ConditionOperator.Equal, "100000000")); // trong va cham soc mia
                    hopdongmia.AddCondition(new ConditionExpression("new_mucdichdautu", ConditionOperator.Equal, "100000002")); // tuoi mia
                    hopdongmia.AddCondition(new ConditionExpression("new_mucdichdautu", ConditionOperator.Equal, "100000003")); // boc la mia
                    qbangLai.Criteria.AddFilter(hopdongmia);
                }

                else if (pbdt.loaihopdong == 100000001) // thue dat
                {
                    FilterExpression hdthuedat = new FilterExpression(LogicalOperator.Or);
                    hdthuedat.AddCondition(new ConditionExpression("new_mucdichdautu", ConditionOperator.Equal, "100000005")); // thue dat
                    qbangLai.Criteria.AddFilter(hdthuedat);
                }
                else if (pbdt.loaihopdong == 100000002) // trang thiet bi
                {
                    FilterExpression hdtrangthietbi = new FilterExpression(LogicalOperator.Or);
                    hdtrangthietbi.AddCondition(new ConditionExpression("new_mucdichdautu", ConditionOperator.Equal, "100000006")); // mua may moc thiet bi
                    qbangLai.Criteria.AddFilter(hdtrangthietbi);
                }
                else if (pbdt.loaihopdong == 100000003) // hd ha tang
                {
                    FilterExpression hdhatang = new FilterExpression(LogicalOperator.Or);
                    hdhatang.AddCondition(new ConditionExpression("new_mucdichdautu", ConditionOperator.Equal, "100000007")); // dau tu ha tang
                    qbangLai.Criteria.AddFilter(hdhatang);
                }

                qbangLai.AddOrder("new_ngayapdung", OrderType.Ascending);
                EntityCollection bls = service.RetrieveMultiple(qbangLai);
                bool isBefore = false;

                if (bls.Entities.Count <= 0)
                {
                    result = (decimal)end.Date.Subtract(begin.Date).TotalDays * (pbdt.laisuat / 3000) * sotien;

                    if (flag == 1) // hinh thuc tra goc
                        CreatePhieutinhlai(sotien, begin, end, pbdt.laisuat, result, pbdt,
                            (int)end.Date.Subtract(begin.Date).TotalDays, 0, sotien, 0);
                    else if (flag == 2) // hinh thuc tra tong tien
                    {
                        if (gocthat > 0)
                            CreatePhieutinhlai(sotien, begin, end, pbdt.laisuat, result, pbdt,
                            (int)end.Date.Subtract(begin.Date).TotalDays, nolai, sotien, pbdt.sotien - sotien);
                    }
                }
                else
                {
                    decimal tmpsn = CompareDate(begin, end);

                    if (tmpsn > 0)
                        throw new Exception("Ngày tính lãi phải lớn hơn hoặc bằng ngày phát sinh hoặc ngày sau cùng tính lãi!");
                    else if (tmpsn == 0)
                        return result;
                    else
                    {
                        #region Find begin & end point
                        point pt1 = null;
                        point pt2 = null;
                        int tmpI = 0;

                        for (int i = 0; i < bls.Entities.Count; i++)
                        {
                            Entity etmp = bls[i];

                            if (!etmp.Contains("new_ngayapdung"))
                                throw new Exception(string.Format("Vui lòng nhập ngày áp dụng cho bảng lãi {0} trên chính sách đầu tư!", etmp["new_name"]));
                            if (!etmp.Contains("new_phantramlaisuat"))
                                throw new Exception(string.Format("Vui lòng nhập lãi suất cho bảng lãi {0} trên chính sách đầu tư!", etmp["new_name"]));
                            DateTime tmp = ((DateTime)etmp["new_ngayapdung"]);

                            if (pt1 == null)
                            {
                                //if (i == 0 && begin.Date < tmp.Date)
                                //    throw new Exception(string.Format("Ngày áp dụng {0:dd-MM-yyyy} không có trên bảng lãi thay đổi. Vui lòng thêm vào!", begin.Date));
                                if (i == 0 && begin.Date < tmp.Date)
                                {
                                    pt1 = new point();
                                    pt1.index = -1;
                                    pt1.sn = (int)((DateTime)bls[0]["new_ngayapdung"]).Date.Subtract(begin.Date).TotalDays;

                                    pt1.isBefore = true;
                                    isBefore = true;
                                }
                                else if (begin.Date < tmp.Date)
                                {
                                    pt1 = new point();
                                    pt1.index = tmpI;
                                    pt1.sn = (int)begin.Date.Subtract(((DateTime)bls[tmpI]["new_ngayapdung"]).Date).TotalDays;
                                }
                                else if (begin.Date == tmp.Date)
                                {
                                    pt1 = new point();
                                    pt1.index = i;
                                    pt1.sn = 0;
                                }
                            }

                            if (pt2 == null)
                            {
                                if (end.Date < tmp.Date)
                                {
                                    pt2 = new point();
                                    pt2.index = tmpI;
                                    pt2.sn = (int)end.Date.Subtract(((DateTime)bls[tmpI]["new_ngayapdung"]).Date).TotalDays;

                                    if (pt1.isBefore == true)
                                        pt2.index++;
                                }
                                else if (end.Date == tmp.Date)
                                {
                                    pt2 = new point();
                                    pt2.index = i;
                                    pt2.sn = 0;

                                    if (pt1.isBefore == true)
                                        pt2.index++;
                                }
                                else
                                {
                                    if (i == bls.Entities.Count - 1)
                                    {
                                        pt2 = new point();
                                        pt2.index = i;
                                        pt2.sn = (int)end.Date.Subtract(tmp.Date).TotalDays;
                                        pt2.isOver = true;

                                        //if (isBefore == true)
                                        //    pt2.index++;

                                    }
                                }
                            }

                            if (pt1 != null && pt2 != null)
                                break;
                            tmpI = i;
                        }
                        #endregion

                        laibandau = isBefore == true ? pbdt.laisuat : (decimal)bls[0]["new_phantramlaisuat"];

                        if (pt1 == null || pt2 == null)
                        {
                            Entity etmp = bls[bls.Entities.Count - 1];
                            List<object> ls1 = new List<object>();

                            int sn = (int)CompareDate(end, begin);
                            decimal pLs = (decimal)etmp["new_phantramlaisuat"];
                            decimal tl = Math.Round((decimal)(sn * pLs * sotien / 3000), MidpointRounding.AwayFromZero);

                            result += tl;
                            ls1.Add(new
                            {
                                sn = sn,
                                en = etmp,
                                ls = pLs,
                                fr = begin,
                                to = end,
                                tl = tl
                            });

                            #region Create phieu tinh lai from ls1

                            if (ls1 != null && ls1.Count > 0)
                            {
                                foreach (object o in ls1)
                                {
                                    var tmp = new { sn = (int)0, en = new Entity(), ls = (decimal)0, fr = DateTime.Now, to = DateTime.Now, tl = (decimal)0 };
                                    tmp = Cast(tmp, o);

                                    if (flag == 1) // tra goc
                                        CreatePhieutinhlai(sotien, tmp.fr, tmp.to, tmp.ls, tmp.tl, pbdt, tmp.sn, 0, sotien, 0);
                                    else if (flag == 2) // tong tien
                                        CreatePhieutinhlai(sotien, tmp.fr, tmp.to, tmp.ls, tmp.tl, pbdt, tmp.sn, nolai, sotien, pbdt.sotien - sotien);

                                }
                            }

                            #endregion

                            return result;
                        }
                        //throw new Exception("Ngày bắt đầu không hợp lệ so với bảng lãi");
                        if (pt1.index == pt2.index)
                        {
                            Entity etmp = bls[pt1.index];
                            laibandau = (decimal)bls[pt1.index]["new_phantramlaisuat"];
                            DateTime tmp = (((DateTime)etmp["new_ngayapdung"])).AddHours(7);
                            DateTime bg = tmp.Date.AddDays(pt1.sn);
                            DateTime ed = tmp.Date.AddDays(pt2.sn);

                            int sn = (int)CompareDate(ed, bg);
                            decimal pLs = (decimal)etmp["new_phantramlaisuat"];
                            decimal tl = Math.Round((decimal)(sn * (pLs > laibandau ? laibandau : pLs) * sotien / 3000), MidpointRounding.AwayFromZero);

                            result += tl;
                            ls.Add(new
                            {
                                sn = sn,
                                en = etmp,
                                ls = pLs,
                                fr = bg,
                                to = ed,
                                tl = tl
                            });
                        }
                        else if (pt2.index - pt1.index == 1)
                        {
                            laibandau = (decimal)bls[pt1.index]["new_phantramlaisuat"];
                            Entity etmp1 = bls[pt1.index];
                            Entity etmp2 = bls[pt2.index];

                            DateTime tmp1 = (((DateTime)etmp1["new_ngayapdung"])).AddHours(7);
                            DateTime tmp2 = (((DateTime)etmp2["new_ngayapdung"])).AddHours(7);

                            DateTime bg = tmp1.Date.AddDays(pt1.sn);
                            int sn = (int)CompareDate(tmp2, bg);
                            decimal pLs = (decimal)etmp1["new_phantramlaisuat"];
                            pLs = pLs > laibandau ? laibandau : pLs;

                            decimal tl = Math.Round((decimal)(sn * pLs * sotien / 3000), MidpointRounding.AwayFromZero);
                            result += tl;
                            ls.Add(new
                            {
                                sn = sn,
                                en = etmp1,
                                ls = pLs,
                                fr = bg,
                                to = tmp2.Date,
                                tl = tl
                            });

                            DateTime ed = tmp2.Date.AddDays(pt2.sn);
                            sn = (int)CompareDate(ed, tmp2);
                            pLs = (decimal)etmp2["new_phantramlaisuat"];
                            pLs = pLs > laibandau ? laibandau : pLs;
                            tl = Math.Round((decimal)(sn * pLs * sotien / 3000), MidpointRounding.AwayFromZero);
                            result += tl;
                            ls.Add(new
                            {
                                sn = sn,
                                en = etmp2,
                                ls = pLs,
                                fr = tmp2.Date,
                                to = ed,
                                tl = tl
                            });
                        }
                        else
                        {
                            Entity cEtn = isBefore == true ? bls[0] : bls[pt1.index];
                            DateTime cDate = cEtn == null ? new DateTime() : (((DateTime)cEtn["new_ngayapdung"])).AddHours(7);

                            if (isBefore == true)
                            {
                                for (int i = pt1.index; i < pt2.index + 1; i++)
                                {
                                    Entity etmp = null;
                                    DateTime tmp = new DateTime();

                                    if (i == -1)
                                    {
                                        tmp = begin;
                                    }
                                    else
                                    {
                                        etmp = bls[i];
                                        tmp = (((DateTime)etmp["new_ngayapdung"])).AddHours(7);
                                    }

                                    if (i == pt1.index)
                                    {
                                        if (i == -1)
                                        {
                                            i++;
                                            Entity etmp1 = bls[i];
                                            DateTime tmp1 = (((DateTime)etmp1["new_ngayapdung"])).AddHours(7);
                                            DateTime bg = begin;
                                            
                                            int sn = (int)CompareDate(tmp1, bg);
                                            decimal pLs = pbdt.laisuat;
                                            pLs = pLs > laibandau ? laibandau : pLs;
                                            decimal tl = Math.Round((decimal)(sn * pLs * sotien / 3000), MidpointRounding.AwayFromZero);

                                            result += tl;
                                            ls.Add(new
                                            {
                                                sn = sn,
                                                en = etmp,
                                                ls = pLs,
                                                fr = bg,
                                                to = tmp1.Date,
                                                tl = tl
                                            });
                                            cDate = tmp1;
                                            cEtn = etmp1;
                                        }
                                        else
                                        {
                                            #region
                                            i++;
                                            Entity etmp1 = bls[i];
                                            DateTime tmp1 = (((DateTime)etmp1["new_ngayapdung"])).AddHours(7);
                                            DateTime bg = tmp.Date.AddDays(pt1.sn);

                                            int sn = (int)CompareDate(tmp1, bg);
                                            decimal pLs = (decimal)cEtn["new_phantramlaisuat"];
                                            pLs = pLs > laibandau ? laibandau : pLs;
                                            decimal tl = Math.Round((decimal)(sn * pLs * sotien / 3000), MidpointRounding.AwayFromZero);

                                            result += tl;
                                            ls.Add(new
                                            {
                                                sn = sn,
                                                en = etmp,
                                                ls = pLs,
                                                fr = bg,
                                                to = tmp1.Date,
                                                tl = tl
                                            });
                                            cDate = tmp1;
                                            cEtn = etmp1;
                                            #endregion
                                        }
                                    }
                                    else if (i != pt2.index)
                                    {
                                        //ls.Add((int)tmp.Date.Subtract(cDate.Date).TotalDays);
                                        int sn = (int)CompareDate(tmp, cDate);
                                        decimal pLs = (decimal)cEtn["new_phantramlaisuat"];
                                        pLs = pLs > laibandau ? laibandau : pLs;
                                        decimal tl = Math.Round((decimal)(sn * pLs * sotien / 3000), MidpointRounding.AwayFromZero);
                                        result += tl;
                                        ls.Add(new
                                        {
                                            sn = sn,
                                            en = cEtn,
                                            ls = pLs,
                                            fr = cDate.Date,
                                            to = tmp.Date,
                                            tl = tl
                                        });
                                        cDate = tmp;
                                        cEtn = etmp;
                                    }
                                    else if (i == pt2.index)
                                    {
                                        if (pt2.isOver == true)
                                        {
                                            int snTmpI = (int)tmp.Date.Subtract(cDate.Date).TotalDays;
                                            decimal pLsTmpI = (decimal)cEtn["new_phantramlaisuat"];
                                            pLsTmpI = pLsTmpI > laibandau ? laibandau : pLsTmpI;
                                            decimal tlTmpI = Math.Round((decimal)(snTmpI * pLsTmpI * sotien / 3000), MidpointRounding.AwayFromZero);
                                            result += tlTmpI;
                                            ls.Add(new
                                            {
                                                sn = snTmpI,
                                                en = cEtn,
                                                ls = pLsTmpI,
                                                fr = cDate,
                                                to = tmp.Date,
                                                tl = tlTmpI
                                            });
                                            cDate = tmp;
                                            cEtn = etmp;

                                            DateTime ed = tmp.Date.AddDays(pt2.sn);
                                            int sn = (int)CompareDate(ed, cDate);
                                            decimal pLs = (decimal)cEtn["new_phantramlaisuat"];
                                            pLs = pLs > laibandau ? laibandau : pLs;
                                            decimal tl = Math.Round((decimal)(sn * pLs * sotien / 3000), MidpointRounding.AwayFromZero);
                                            result += tl;
                                            ls.Add(new
                                            {
                                                sn = sn,
                                                en = cEtn,
                                                ls = pLs,
                                                fr = cDate.Date,
                                                to = ed,
                                                tl = tl
                                            });
                                        }
                                        else
                                        {
                                            DateTime ed = tmp.Date.AddDays(pt2.sn);
                                            int sn = (int)CompareDate(ed, cDate);
                                            decimal pLs = (decimal)cEtn["new_phantramlaisuat"];
                                            pLs = pLs > laibandau ? laibandau : pLs;
                                            decimal tl = Math.Round((decimal)(sn * pLs * sotien / 3000), MidpointRounding.AwayFromZero);
                                            result += tl;
                                            ls.Add(new
                                            {
                                                sn = sn,
                                                en = cEtn,
                                                ls = pLs,
                                                fr = cDate.Date,
                                                to = ed,
                                                tl = tl
                                            });
                                        }
                                        break;
                                    }
                                }
                            }
                            else
                            {
                                for (int i = pt1.index; i < pt2.index + 1; i++)
                                {
                                    Entity etmp = bls[i];
                                    DateTime tmp = (((DateTime)etmp["new_ngayapdung"])).AddHours(7);

                                    if (i == pt1.index)
                                    {
                                        i++;
                                        Entity etmp1 = bls[i];
                                        DateTime tmp1 = (((DateTime)etmp1["new_ngayapdung"])).AddHours(7);
                                        DateTime bg = tmp.Date.AddDays(pt1.sn);

                                        int sn = (int)CompareDate(tmp1, bg);
                                        decimal pLs = (decimal)cEtn["new_phantramlaisuat"];
                                        pLs = pLs > laibandau ? laibandau : pLs;
                                        decimal tl = Math.Round((decimal)(sn * pLs * sotien / 3000), MidpointRounding.AwayFromZero);

                                        result += tl;
                                        ls.Add(new
                                        {
                                            sn = sn,
                                            en = etmp,
                                            ls = pLs,
                                            fr = bg,
                                            to = tmp1.Date,
                                            tl = tl
                                        });
                                        cDate = tmp1;
                                        cEtn = etmp1;

                                    }
                                    else if (i != pt2.index)
                                    {
                                        //ls.Add((int)tmp.Date.Subtract(cDate.Date).TotalDays);
                                        int sn = (int)CompareDate(tmp, cDate);
                                        decimal pLs = (decimal)cEtn["new_phantramlaisuat"];
                                        pLs = pLs > laibandau ? laibandau : pLs;
                                        decimal tl = Math.Round((decimal)(sn * pLs * sotien / 3000), MidpointRounding.AwayFromZero);
                                        result += tl;
                                        ls.Add(new
                                        {
                                            sn = sn,
                                            en = cEtn,
                                            ls = pLs,
                                            fr = cDate.Date,
                                            to = tmp.Date,
                                            tl = tl
                                        });
                                        cDate = tmp;
                                        cEtn = etmp;
                                    }
                                    else if (i == pt2.index)
                                    {
                                        if (pt2.isOver == true)
                                        {
                                            int snTmpI = (int)tmp.Date.Subtract(cDate.Date).TotalDays;
                                            decimal pLsTmpI = (decimal)cEtn["new_phantramlaisuat"];
                                            pLsTmpI = pLsTmpI > laibandau ? laibandau : pLsTmpI;
                                            decimal tlTmpI = Math.Round((decimal)(snTmpI * pLsTmpI * sotien / 3000), MidpointRounding.AwayFromZero);
                                            result += tlTmpI;
                                            ls.Add(new
                                            {
                                                sn = snTmpI,
                                                en = cEtn,
                                                ls = pLsTmpI,
                                                fr = cDate,
                                                to = tmp.Date,
                                                tl = tlTmpI
                                            });
                                            cDate = tmp;
                                            cEtn = etmp;

                                            DateTime ed = tmp.Date.AddDays(pt2.sn);
                                            int sn = (int)CompareDate(ed, cDate);
                                            decimal pLs = (decimal)cEtn["new_phantramlaisuat"];
                                            pLs = pLs > laibandau ? laibandau : pLs;
                                            decimal tl = Math.Round((decimal)(sn * pLs * sotien / 3000), MidpointRounding.AwayFromZero);
                                            result += tl;
                                            ls.Add(new
                                            {
                                                sn = sn,
                                                en = cEtn,
                                                ls = pLs,
                                                fr = cDate.Date,
                                                to = ed,
                                                tl = tl
                                            });
                                        }
                                        else
                                        {
                                            DateTime ed = tmp.Date.AddDays(pt2.sn);
                                            int sn = (int)CompareDate(ed, cDate);
                                            decimal pLs = (decimal)cEtn["new_phantramlaisuat"];
                                            pLs = pLs > laibandau ? laibandau : pLs;
                                            decimal tl = Math.Round((decimal)(sn * pLs * sotien / 3000), MidpointRounding.AwayFromZero);
                                            result += tl;
                                            ls.Add(new
                                            {
                                                sn = sn,
                                                en = cEtn,
                                                ls = pLs,
                                                fr = cDate.Date,
                                                to = ed,
                                                tl = tl
                                            });
                                        }
                                        break;
                                    }
                                }
                            }
                        }

                        #region Create phieu tinh lai from ls

                        if (ls != null && ls.Count > 0)
                        {
                            foreach (object o in ls)
                            {
                                var tmp = new { sn = (int)0, en = new Entity(), ls = (decimal)0, fr = DateTime.Now, to = DateTime.Now, tl = (decimal)0 };
                                tmp = Cast(tmp, o);

                                if (flag == 1 && tmp.tl > 0) // tra goc
                                    CreatePhieutinhlai(sotien, tmp.fr, tmp.to, tmp.ls, tmp.tl, pbdt, tmp.sn, 0, sotien, 0);
                                else if (flag == 2 && tmp.tl > 0) // hinh thuc tra tong tien
                                {
                                    if (gocthat > 0)
                                    {
                                        if (cachtinhlai == 2) // lai tren tong tien
                                        {
                                            CreatePhieutinhlai(sotien, tmp.fr, tmp.to, tmp.ls, tmp.tl, pbdt, tmp.sn, nolai, gocthat, pbdt.sotien - sotien);
                                        }
                                        else // lai tren tien thu
                                        {
                                            CreatePhieutinhlai(sotien, tmp.fr, tmp.to, tmp.ls, tmp.tl, pbdt, tmp.sn, nolai, sotien, pbdt.sotien - sotien);
                                        }

                                    }
                                }
                            }
                        }

                        #endregion

                        return result;
                    }
                }
            }
            return result;
        }

        private decimal nolai = 0;
        private decimal gocthat = 0;
        private int cachtinhlai = -1;

        private decimal CompareDate(DateTime date1, DateTime date2) // begin,end
        {
            string currentTimerZone = TimeZoneInfo.Local.Id;
            DateTime d1 = TimeZoneInfo.ConvertTimeBySystemTimeZoneId(date1, currentTimerZone);
            DateTime d2 = TimeZoneInfo.ConvertTimeBySystemTimeZoneId(date2, currentTimerZone);
            decimal temp = (decimal)d1.Date.Subtract(d2.Date).TotalDays;
            return temp;
        }
        private void Tragoc_Laitrentienthu(List<Phanbodautu> lstPbdt, decimal tragoc, Entity target)
        {
            ClearAllPhieutinhlai();
            decimal Totallai = 0;
            decimal TongtienScope = 0;
            flag = 1;
            decimal tonggoc = 0;

            foreach (Phanbodautu pbdt in lstPbdt)
            {
                if (pbdt.sotien == 0 || pbdt.loailaisuat == 0)
                    continue;
                gocthat = 0;
                DateTime bg = pbdt.ngayphatsinh;

                if (TongtienScope >= tragoc)
                    break;

                decimal sotientinhlai = pbdt.sotien > (tragoc - TongtienScope) ? (tragoc - TongtienScope) : pbdt.sotien;
                TongtienScope += sotientinhlai;
                gocthat = sotientinhlai;

                decimal tl = Tinhlaitungphieuphanbo(pbdt, sotientinhlai, bg, ngaythu, flag);
                Totallai += tl;
                tonggoc += sotientinhlai;
            }

            tongtien = tonggoc + Totallai;
            tonglai = Totallai;

            Entity targetUpdate = service.Retrieve(target.LogicalName, target.Id,
                new ColumnSet(new string[] { "new_thulai", "new_tongtienthu", "new_sotiencondu" }));

            targetUpdate["new_thulai"] = new Money(Totallai);
            targetUpdate["new_tongtienthu"] = new Money(tongtien);
            targetUpdate["new_sotiencondu"] = new Money((tragoc - tonggoc) > 0 ? tragoc - tonggoc : 0);
            service.Update(targetUpdate);
        }

        private void Tragoc_Laitrentongtien(List<Phanbodautu> lstPbdt, decimal tragoc, Entity target)
        {
            ClearAllPhieutinhlai();

            decimal Totallai = 0;
            decimal TongtienScope = 0;
            decimal tl = 0;
            flag = 1;

            foreach (Phanbodautu pbdt in lstPbdt)
            {
                if (pbdt.sotien == 0)
                    continue;

                gocthat = 0;
                DateTime bg = pbdt.ngaytinhlaisaucung;

                decimal sotientinhlai = pbdt.sotien >= (tragoc - TongtienScope) ? (tragoc - TongtienScope) : pbdt.sotien;
                TongtienScope += sotientinhlai;
                gocthat = sotientinhlai;

                tl = Tinhlaitungphieuphanbo(pbdt, pbdt.sotien, bg, ngaythu, flag = 1);
                Totallai += tl;
            }

            tongtien = tragoc + Totallai;
            tonglai = Totallai;

            Entity targetUpdate = service.Retrieve(target.LogicalName, target.Id,
                new ColumnSet(new string[] { "new_thulai", "new_tongtienthu" }));

            targetUpdate["new_thulai"] = new Money(tonglai);
            targetUpdate["new_tongtienthu"] = new Money(tongtien);
            service.Update(targetUpdate);
        }

        private void Tongtien_Laitrentienthu(List<Phanbodautu> lstPbdt, decimal tongtien, Entity target)
        {
            ClearAllPhieutinhlai();

            decimal TongtienScope = 0;
            decimal Totallai = 0;
            decimal sotientinhlai = 0;
            decimal tonggoc = 0;
            bool t = true;

            foreach (Phanbodautu pbdt in lstPbdt)
            {
                if (pbdt.sotien == 0)
                    continue;

                DateTime bg = pbdt.ngayphatsinh;

                decimal tl = Tinhlaitungphieuphanbo(pbdt, pbdt.sotien, bg, ngaythu, flag = -1);
                sotientinhlai = (pbdt.sotien + tl) > (tongtien - TongtienScope) ? (tongtien - TongtienScope) : pbdt.sotien;
                TongtienScope += sotientinhlai;
                gocthat = tongtien - TongtienScope;

                TongtienScope += tl;

                //if (tongtien < sumlai)
                //{
                //    CreatePhieutinhlaiOnlyNoLai(pbdt, sumlai);
                //    t = false;
                //    break;
                //}

                if (TongtienScope >= tongtien)
                {
                    decimal tl1 = Tinhlaitungphieuphanbo(pbdt, pbdt.sotien, bg, ngaythu, flag = -1);
                    decimal goccuoi = sotientinhlai * pbdt.sotien / (tl1 + pbdt.sotien);
                    tonggoc += goccuoi;
                    gocthat = goccuoi;
                    Totallai += Tinhlaitungphieuphanbo(pbdt, goccuoi, bg, ngaythu, flag = 2);
                    break;
                }

                gocthat = sotientinhlai;
                decimal tl3 = Tinhlaitungphieuphanbo(pbdt, sotientinhlai, bg, ngaythu, flag = 2);
                tonggoc += sotientinhlai;

                Totallai += tl3;
            }

            decimal tonggoc1 = TongGoc(lstPbdt);
            tragoc = t == true ? (tongtien - Totallai) : 0;
            decimal sotiencondu = 0;

            if (tonggoc1 < tragoc)
            {
                sotiencondu = tragoc - tonggoc1;
                tragoc = tonggoc;
            }
            else
                sotiencondu = 0;

            tonglai = Totallai;

            Entity targetUpdate = service.Retrieve(target.LogicalName, target.Id,
                new ColumnSet(new string[] { "new_thulai", "new_tongtienthu", "new_thunogoc", "new_sotiencondu" }));

            targetUpdate["new_thulai"] = new Money(tonglai);
            targetUpdate["new_tongtienthu"] = new Money(tongtien);
            targetUpdate["new_thunogoc"] = new Money(tragoc);
            targetUpdate["new_sotiencondu"] = new Money(sotiencondu);
            service.Update(targetUpdate);
        }

        private void Tongtien_Laitrentongtien(List<Phanbodautu> lstPbdt, decimal tongtien, Entity target)
        {
            ClearAllPhieutinhlai();

            decimal TongtienScope = 0;
            decimal Totallai = 0;
            decimal sotientinhlai = 0;
            cachtinhlai = 2; // lai tren tong tien

            bool t = true;
            decimal sumlai = Sumlai(lstPbdt);

            foreach (Phanbodautu pbdt in lstPbdt)
            {
                if (pbdt.sotien == 0)
                    continue;

                if (tongtien < sumlai)
                {
                    CreatePhieutinhlaiOnlyNoLai(pbdt, sumlai - tongtien);
                    t = false;
                    break;
                }

                gocthat = 0;

                DateTime bg = pbdt.ngaytinhlaisaucung;

                sotientinhlai = pbdt.sotien >= (tragoc - TongtienScope) ? (tragoc - TongtienScope) : pbdt.sotien;
                TongtienScope += sotientinhlai;
                gocthat = sotientinhlai;

                decimal tl = Tinhlaitungphieuphanbo(pbdt, pbdt.sotien, bg, ngaythu, flag = 1);
                Totallai += tl;
            }

            if (t == false)
            {
                tonglai = tongtien;
                tragoc = 0;
            }
            else
            {
                tragoc = tongtien - Totallai;
                tonglai = Totallai;
            }

            Entity targetUpdate = service.Retrieve(target.LogicalName, target.Id,
                new ColumnSet(new string[] { "new_thulai", "new_tongtienthu", "new_thunogoc", "new_sotiencondu" }));

            targetUpdate["new_thulai"] = new Money(tonglai);
            targetUpdate["new_tongtienthu"] = new Money(tongtien);
            targetUpdate["new_thunogoc"] = new Money(tragoc);
            //targetUpdate["new_sotiencondu"] = new Money(sotiencondu);
            service.Update(targetUpdate);
        }
        private void CreatePhieutinhlaiOnlyNoLai(Phanbodautu pbdt, decimal _nolai)
        {
            Entity phieutinhlai = new Entity("new_phieutinhlai");

            phieutinhlai["new_tienvay"] = new Money(pbdt.sotien);
            phieutinhlai["new_nolai"] = new Money(_nolai);
            phieutinhlai["new_phanbodautu"] = new EntityReference("new_phanbodautu", pbdt.ID);
            phieutinhlai["new_phieudenghithuno"] = new EntityReference("new_phieudenghithuno", phieudenghithunoID);
            phieutinhlai["new_trgc"] = new Money(0);
            phieutinhlai["new_ngc"] = new Money(pbdt.sotien);

            Guid pID = service.Create(phieutinhlai);

            EntityReferenceCollection t = new EntityReferenceCollection();

            t.Add(new EntityReference(phieutinhlai.LogicalName, pID));

            service.Associate("new_phieudenghithuno", phieudenghithunoID, new Relationship("new_new_phieutinhlai_new_phieudenghithuno"),
                t);
        }

        private void CreatePhieutinhlai(decimal tienvay, DateTime ngayvay, DateTime ngaytra,
            decimal laisuat, decimal tienlai, Phanbodautu pbdt, int songay, decimal nolai, decimal datragoc, decimal nogoc)
        {
            Entity phieutinhlai = new Entity("new_phieutinhlai");
            phieutinhlai["new_tienvay"] = new Money(tienvay);
            phieutinhlai["new_ngayvay"] = ngayvay;
            phieutinhlai["new_ngaytra"] = ngaytra;
            phieutinhlai["new_laisuat"] = laisuat;
            phieutinhlai["new_tienlai"] = new Money(tienlai);
            phieutinhlai["new_nolai"] = new Money(nolai);
            phieutinhlai["new_trgc"] = new Money(datragoc);
            phieutinhlai["new_ngc"] = new Money(nogoc);
            phieutinhlai["new_phanbodautu"] = new EntityReference("new_phanbodautu", pbdt.ID);
            phieutinhlai["new_songay"] = songay;
            phieutinhlai["new_phieudenghithuno"] = new EntityReference("new_phieudenghithuno", phieudenghithunoID);

            Guid pID = service.Create(phieutinhlai);

            EntityReferenceCollection t = new EntityReferenceCollection();

            t.Add(new EntityReference(phieutinhlai.LogicalName, pID));

            service.Associate("new_phieudenghithuno", phieudenghithunoID, new Relationship("new_new_phieutinhlai_new_phieudenghithuno"),
                t);
        }

        private static T Cast<T>(T typeHolder, object x)
        {
            // typeHolder above is just for compiler magic
            // to infer the type to cast x to
            return (T)x;
        }

        private Entity Clone(Entity entity)
        {
            Entity en = new Entity(entity.LogicalName);
            en.Id = entity.Id;
            foreach (string attr in entity.Attributes.Keys)
            {
                en[attr] = entity[attr];
            }
            return en;
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

        private decimal Sumlai(List<Phanbodautu> lstPbdt)
        {
            decimal result = 0;
            decimal TongtienScope = 0;
            decimal tl = 0;
            flag = -1;

            foreach (Phanbodautu pbdt in lstPbdt)
            {
                if (pbdt.sotien == 0)
                    continue;

                DateTime bg = pbdt.ngaytinhlaisaucung;

                decimal sotientinhlai = pbdt.sotien >= (tragoc - TongtienScope) ? (tragoc - TongtienScope) : pbdt.sotien;
                TongtienScope += sotientinhlai;

                tl = Tinhlaitungphieuphanbo(pbdt, pbdt.sotien, bg, ngaythu, flag);
                result += tl;
            }

            return result;
        }

        private bool CheckSotienlonhontiengoc(List<Phanbodautu> lstPBDT, decimal tongtien)
        {
            decimal result = 0;

            foreach (Phanbodautu en in lstPBDT)
            {
                result += en.sotien;
            }

            return (tongtien > result);
        }

        private decimal TongGoc(List<Phanbodautu> lstPBDT)
        {
            decimal result = 0;

            foreach (Phanbodautu en in lstPBDT)
            {
                result += en.sotien;
            }

            return result;
        }
    }
}
