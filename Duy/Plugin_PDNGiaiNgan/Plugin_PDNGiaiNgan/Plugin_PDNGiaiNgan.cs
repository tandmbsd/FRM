using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk.Messages;

namespace Plugin_PDNGiaiNgan
{
    public class Plugin_PDNGiaiNgan : IPlugin
    {
        decimal sum(EntityCollection thuacanhtac)
        {
            decimal temp = 0;
            foreach (Entity en in thuacanhtac.Entities)
            {
                temp += en.Contains("new_dientichthucte") ? en.GetAttributeValue<decimal>("new_dientichthucte") : 0;
            }

            return temp;
        }

        decimal sumnt(EntityCollection thuacanhtac)
        {
            decimal temp = 0;
            foreach (Entity en in thuacanhtac.Entities)
            {
                temp += en.Contains("new_dientichnghiemthu") ? en.GetAttributeValue<decimal>("new_dientichnghiemthu") : 0;
            }

            return temp;
        }

        decimal sumtuoi(EntityCollection thuacanhtac)
        {
            decimal temp = 0;
            foreach (Entity en in thuacanhtac.Entities)
            {
                temp += en.Contains("new_dientichtuoi") ? en.GetAttributeValue<decimal>("new_dientichtuoi") : 0;
            }

            return temp;
        }

        decimal sumboclamia(EntityCollection thuacanhtac)
        {
            decimal temp = 0;
            foreach (Entity en in thuacanhtac.Entities)
            {
                temp += en.Contains("new_dientich") ? en.GetAttributeValue<decimal>("new_dientich") : 0;
            }

            return temp;
        }

        Money summmtb(EntityCollection chitiethopdongdaututrangthietbi)
        {
            Money kq = new Money(0);
            foreach (Entity en in chitiethopdongdaututrangthietbi.Entities)
            {
                kq = new Money(((Money)en["new_giatrihopdong"]).Value + ((Money)kq).Value);
            }
            return kq;
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

        IOrganizationService service = null;
        IOrganizationServiceFactory factory = null;
        void IPlugin.Execute(IServiceProvider serviceProvider)
        {
            try
            {
                IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
                factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
                service = factory.CreateOrganizationService(context.UserId);

                Entity target = (Entity)context.InputParameters["Target"];

                if (target.Contains("statuscode") && ((OptionSetValue)target["statuscode"]).Value == 100000000)
                {
                    Entity PDNGN = service.Retrieve(target.LogicalName, target.Id, new ColumnSet(new string[] { "new_phieudenghigiainganid", "new_masophieu", "new_loaihopdong", "new_hopdongdautumia", "new_vudautu", "statuscode", "new_khachhang", "new_khachhangdoanhnghiep", "new_sotiendthoanlai", "new_sotiendtkhonghoanlai" }));
                    string loaihopdong = ((OptionSetValue)PDNGN["new_loaihopdong"]).Value.ToString();

                    string tinhtrang = ((OptionSetValue)PDNGN["statuscode"]).Value.ToString();
                    #region hop dong dau tu mia
                    if (loaihopdong == "100000000") // hop dong dau tu mia
                    {
                        List<Entity> ChiTietPDNGN = RetrieveMultiRecord(service, "new_chitietphieudenghigiaingan", new ColumnSet(true), "new_phieudenghigiaingan", PDNGN.Id);
                        decimal sotiendthoanlai = ((Money)PDNGN["new_sotiendthoanlai"]).Value;
                        decimal sotiendtkhonghoanlai = ((Money)PDNGN["new_sotiendtkhonghoanlai"]).Value;

                        Entity hddtm = null;
                        if (PDNGN.Contains("new_hopdongdautumia"))
                        {
                            hddtm = service.Retrieve("new_hopdongdautumia", ((EntityReference)PDNGN["new_hopdongdautumia"]).Id, new ColumnSet(true));
                        }
                        if (hddtm != null)
                        {
                            decimal tongchihoanlai = hddtm.Contains("new_tongchihoanlai") ? ((Money)hddtm["new_tongchihoanlai"]).Value : 0;
                            decimal tongchikhonghoanlai = hddtm.Contains("new_tongchikhonghoanlai") ? ((Money)hddtm["new_tongchikhonghoanlai"]).Value : 0;
                            decimal dinhmuchoanlaihientai = hddtm.Contains("new_dinhmucdautuhoanlai_hientai") ? ((Money)hddtm["new_dinhmucdautuhoanlai_hientai"]).Value : 0;
                            decimal dinhmuckhonghoanlaihientai = hddtm.Contains("new_dinhmucdautukhonghoanlai_hientai") ? ((Money)hddtm["new_dinhmucdautukhonghoanlai_hientai"]).Value : 0;
                            decimal dinhmuchoanlaibandau = hddtm.Contains("new_dinhmucdautucohoanlai") ? ((Money)hddtm["new_dinhmucdautucohoanlai"]).Value : 0;
                            decimal dinhmuckhonghoanlaibandau = hddtm.Contains("new_dinhmucdautukhonghoanlai") ? ((Money)hddtm["new_dinhmucdautukhonghoanlai"]).Value : 0;

                            if (dinhmuchoanlaihientai != 0)
                            {
                                if (sotiendthoanlai + tongchihoanlai > dinhmuchoanlaihientai)
                                {
                                    throw new Exception("Số tiền giải ngân vượt định mức đầu tư hoàn lại");
                                }
                            }
                            else
                            {
                                if (sotiendthoanlai + tongchihoanlai > dinhmuchoanlaibandau)
                                {
                                    throw new Exception("Số tiền giải ngân vượt định mức đầu tư hoàn lại");
                                }
                            }

                            if (dinhmuchoanlaihientai != 0)
                            {
                                if (sotiendtkhonghoanlai + tongchikhonghoanlai > dinhmuckhonghoanlaihientai)
                                {
                                    throw new Exception("Số tiền giải ngân vượt định mức đầu tư không hoàn lại");
                                }
                            }
                            else
                            {
                                if (sotiendtkhonghoanlai + tongchikhonghoanlai > dinhmuckhonghoanlaibandau)
                                {
                                    throw new Exception("Số tiền giải ngân vượt định mức đầu tư không hoàn lại");
                                }
                            }
                        }
                        
                        foreach (Entity ct in ChiTietPDNGN)
                        {
                            decimal tongtien = ct.Contains("new_sotiendtkhonghoanlai") ? ((Money)ct["new_sotiendtkhonghoanlai"]).Value : ct.Contains("new_sotiendautucohoanlaivattu") ? ((Money)ct["new_sotiendautucohoanlaivattu"]).Value : ct.Contains("new_sotiendautucohoanlaitienmat") ? ((Money)ct["new_sotiendautucohoanlaitienmat"]).Value : 0;

                            string loaidautu = ((OptionSetValue)ct["new_loaidautu"]).Value.ToString();
                            #region PGN_homgiong
                            if (((OptionSetValue)ct["new_noidunggiaingan"]).Value.ToString() == "100000000") // PGN hôm giống
                            {
                                Entity PGNHG = service.Retrieve("new_phieugiaonhanhomgiong", ((EntityReference)ct["new_phieugiaonhanhomgiong"]).Id, new ColumnSet(true));
                                string loaigiaonhanhom = ((OptionSetValue)PGNHG["new_loaigiaonhanhom"]).Value.ToString();
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

                                    if (PDNGN.Contains("new_khachhang"))
                                    {
                                        phanbodautu["new_khachhang"] = PDNGN["new_khachhang"];
                                    }
                                    else
                                    {
                                        phanbodautu["new_khachhangdoanhnghiep"] = PDNGN["new_khachhangdoanhnghiep"];
                                    }

                                    if (((OptionSetValue)PDNGN["new_loaihopdong"]).Value.ToString() == "100000000")
                                    {
                                        phanbodautu["new_loaihopdong"] = new OptionSetValue(100000001);
                                        phanbodautu["new_thuacanhtac"] = en.ToEntityReference();
                                    }
                                    else if (((OptionSetValue)PDNGN["new_loaihopdong"]).Value.ToString() == "100000001")
                                    {
                                        phanbodautu["new_loaihopdong"] = new OptionSetValue(100000002);
                                    }
                                    else if (((OptionSetValue)PDNGN["new_loaihopdong"]).Value.ToString() == "100000002")
                                    {
                                        phanbodautu["new_loaihopdong"] = new OptionSetValue(100000004);
                                    }

                                    Entity vudautu = service.Retrieve("new_vudautu", ((EntityReference)PDNGN["new_vudautu"]).Id, new ColumnSet(true));
                                    phanbodautu["new_hopdongdautumia"] = en["new_hopdongdautumia"];
                                    phanbodautu["new_vudautu"] = PDNGN["new_vudautu"];
                                    //phanbodautu["new_phieuchitienmat"] = new EntityReference(target.LogicalName, target.Id);
                                    phanbodautu["new_ngayphatsinh"] = DateTime.Now;
                                    phanbodautu["new_datra"] = new Money(0);
                                    phanbodautu["new_loaidautu"] = ((OptionSetValue)ct["new_loaidautu"]).Value.ToString() == "100000000" ? new OptionSetValue(100000001) : new OptionSetValue(100000000);

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
                                if (sotienthat != tongtien)
                                {
                                    Entity pbdttemp = service.Retrieve("new_phanbodautu", id, new ColumnSet(true));
                                    pbdttemp["new_sotien"] = new Money(((Money)pbdttemp["new_sotien"]).Value - (sotienthat - tongtien));
                                    service.Update(pbdttemp);
                                }
                            }
                            #endregion
                            #region PGN_phanbon
                            else if (((OptionSetValue)ct["new_noidunggiaingan"]).Value.ToString() == "100000001") // PGN Phân bón
                            {
                                Entity PGNPB = service.Retrieve("new_phieugiaonhanphanbon", ((EntityReference)ct["new_phieugiaonhanphanbon"]).Id, new ColumnSet(true));
                                decimal sotienthat = 0;
                                Guid id = new Guid();
                                EntityCollection chitiethddtm = RetrieveNNRecord(service, "new_thuadatcanhtac", "new_phieugiaonhanphanbon", "new_new_pgnphanbon_new_chitiethddtmia", new ColumnSet(new string[] { "new_khachhang", "new_khachhangdoanhnghiep", "new_hopdongdautumia", "new_dientichthucte", "new_chinhsachdautu", "new_co", "new_no", "new_name" }), "new_phieugiaonhanphanbonid", PGNPB.Id);

                                foreach (Entity en in chitiethddtm.Entities)
                                {
                                    if (!en.Contains("new_chinhsachdautu"))
                                    {
                                        throw new Exception(en["new_name"].ToString() + " chưa có chính sách đầu tư");
                                    }
                                    Entity chinhsachdautu = service.Retrieve("new_chinhsachdautu", ((EntityReference)en["new_chinhsachdautu"]).Id, new ColumnSet(true));
                                    List<Entity> tilethuhoi = RetrieveMultiRecord(service, "new_tilethuhoivon", new ColumnSet(new string[] { "new_phantramtilethuhoi", "new_nam" }), "new_chinhsachdautu", chinhsachdautu.Id);

                                    Entity phanbodautu = new Entity("new_phanbodautu");

                                    if (PDNGN.Contains("new_khachhang"))
                                    {
                                        phanbodautu["new_khachhang"] = PDNGN["new_khachhang"];
                                    }
                                    else
                                    {
                                        phanbodautu["new_khachhangdoanhnghiep"] = PDNGN["new_khachhangdoanhnghiep"];
                                    }

                                    if (((OptionSetValue)PDNGN["new_loaihopdong"]).Value.ToString() == "100000000")
                                    {
                                        phanbodautu["new_loaihopdong"] = new OptionSetValue(100000001);
                                        phanbodautu["new_thuacanhtac"] = en.ToEntityReference();
                                    }
                                    else if (((OptionSetValue)PDNGN["new_loaihopdong"]).Value.ToString() == "100000001")
                                    {
                                        phanbodautu["new_loaihopdong"] = new OptionSetValue(100000002);
                                    }
                                    else if (((OptionSetValue)PDNGN["new_loaihopdong"]).Value.ToString() == "100000002")
                                    {
                                        phanbodautu["new_loaihopdong"] = new OptionSetValue(100000004);
                                    }

                                    Entity vudautu = service.Retrieve("new_vudautu", ((EntityReference)PDNGN["new_vudautu"]).Id, new ColumnSet(true));
                                    phanbodautu["new_hopdongdautumia"] = en["new_hopdongdautumia"];
                                    phanbodautu["new_vudautu"] = PDNGN["new_vudautu"];
                                    //phanbodautu["new_phieuchitienmat"] = new EntityReference(target.LogicalName, target.Id);
                                    phanbodautu["new_ngayphatsinh"] = DateTime.Now;
                                    phanbodautu["new_datra"] = new Money(0);
                                    phanbodautu["new_loaidautu"] = ((OptionSetValue)ct["new_loaidautu"]).Value.ToString() == "100000000" ? new OptionSetValue(100000001) : new OptionSetValue(100000000);
                                    if (tilethuhoi.Count == 0)
                                    {
                                        throw new Exception("Chính sách không có tỉ lệ thu hồi !!!");
                                    }
                                    foreach (Entity tlth in tilethuhoi)
                                    {
                                        Money k = new Money(Math.Ceiling((tongtien / sum(chitiethddtm) * en.GetAttributeValue<decimal>("new_dientichthucte") * tlth.GetAttributeValue<decimal>("new_phantramtilethuhoi") / new decimal(100))));
                                        phanbodautu["new_sotien"] = k;
                                        sotienthat += k.Value;
                                        DateTime dt = ((DateTime)vudautu["new_ngayketthucvuthuhoach"]).ToLocalTime();
                                        phanbodautu["new_hanthanhtoan"] = new DateTime(tlth.GetAttributeValue<int>("new_nam"), dt.Month, dt.Day);

                                        id = service.Create(phanbodautu);

                                        Entity phanbodautu1 = service.Retrieve("new_phanbodautu", id, new ColumnSet(new string[] { "new_maphieuphanbo", "new_name", "new_datra", "new_conlai", "new_sotien" }));

                                        phanbodautu1["new_name"] = "PBĐT" + phanbodautu1["new_maphieuphanbo"].ToString();
                                        phanbodautu1["new_conlai"] = new Money((decimal)((Money)phanbodautu1["new_sotien"]).Value - (decimal)((Money)phanbodautu1["new_datra"]).Value);
                                        DateTime dtStart = vudautu.GetAttributeValue<DateTime>("new_ngaybatdau");
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
                                        Entity vudautuhientai = RetrieveSingleRecord(service, "new_vudautu", new ColumnSet(true), "new_danghoatdong", true);

                                        service.Update(phanbodautu1);
                                        if (en.Contains("new_co") == false)
                                        {
                                            en["new_co"] = new Money(0);
                                        }

                                        en["new_co"] = new Money(((Money)phanbodautu1["new_sotien"]).Value + ((Money)en["new_co"]).Value);
                                        service.Update(en);
                                    }
                                }
                                if (sotienthat != tongtien)
                                {
                                    Entity pbdttemp = service.Retrieve("new_phanbodautu", id, new ColumnSet(true));
                                    pbdttemp["new_sotien"] = new Money(((Money)pbdttemp["new_sotien"]).Value - (sotienthat - tongtien));
                                    service.Update(pbdttemp);
                                }
                            }
                            #endregion
                            #region PGN_thuoc
                            else if (((OptionSetValue)ct["new_noidunggiaingan"]).Value.ToString() == "100000002") // PGN Thuốc
                            {
                                Entity PGNT = service.Retrieve("new_phieugiaonhanthuoc", ((EntityReference)ct["new_phieugiaonhanthuoc"]).Id, new ColumnSet(true));
                                decimal sotienthat = 0;
                                Guid id = new Guid();
                                EntityCollection chitiethddtm = RetrieveNNRecord(service, "new_thuadatcanhtac", "new_phieugiaonhanthuoc", "new_new_pgnthuoc_new_chitiethddtmia", new ColumnSet(new string[] { "new_khachhang", "new_khachhangdoanhnghiep", "new_hopdongdautumia", "new_dientichthucte", "new_chinhsachdautu", "new_no", "new_co", "new_name" }), "new_phieugiaonhanthuocid", PGNT.Id);

                                foreach (Entity en in chitiethddtm.Entities)
                                {
                                    if (!en.Contains("new_chinhsachdautu"))
                                    {
                                        throw new Exception(en["new_name"].ToString() + " chưa có chính sách đầu tư");
                                    }
                                    Entity chinhsachdautu = service.Retrieve("new_chinhsachdautu", ((EntityReference)en["new_chinhsachdautu"]).Id, new ColumnSet(true));
                                    List<Entity> tilethuhoi = RetrieveMultiRecord(service, "new_tilethuhoivon", new ColumnSet(new string[] { "new_phantramtilethuhoi", "new_nam" }), "new_chinhsachdautu", chinhsachdautu.Id);

                                    Entity phanbodautu = new Entity("new_phanbodautu");

                                    if (PDNGN.Contains("new_khachhang"))
                                    {
                                        phanbodautu["new_khachhang"] = PDNGN["new_khachhang"];
                                    }
                                    else
                                    {
                                        phanbodautu["new_khachhangdoanhnghiep"] = PDNGN["new_khachhangdoanhnghiep"];
                                    }

                                    if (((OptionSetValue)PDNGN["new_loaihopdong"]).Value.ToString() == "100000000")
                                    {
                                        phanbodautu["new_loaihopdong"] = new OptionSetValue(100000001);
                                        phanbodautu["new_thuacanhtac"] = en.ToEntityReference();
                                    }
                                    else if (((OptionSetValue)PDNGN["new_loaihopdong"]).Value.ToString() == "100000001")
                                    {
                                        phanbodautu["new_loaihopdong"] = new OptionSetValue(100000002);
                                    }
                                    else if (((OptionSetValue)PDNGN["new_loaihopdong"]).Value.ToString() == "100000002")
                                    {
                                        phanbodautu["new_loaihopdong"] = new OptionSetValue(100000004);
                                    }

                                    Entity vudautu = service.Retrieve("new_vudautu", ((EntityReference)PDNGN["new_vudautu"]).Id, new ColumnSet(true));
                                    phanbodautu["new_hopdongdautumia"] = en["new_hopdongdautumia"];
                                    phanbodautu["new_vudautu"] = PDNGN["new_vudautu"];
                                    //phanbodautu["new_phieuchitienmat"] = new EntityReference(target.LogicalName, target.Id);
                                    phanbodautu["new_ngayphatsinh"] = DateTime.Now;
                                    phanbodautu["new_datra"] = new Money(0);
                                    phanbodautu["new_loaidautu"] = ((OptionSetValue)ct["new_loaidautu"]).Value.ToString() == "100000000" ? new OptionSetValue(100000001) : new OptionSetValue(100000000);
                                    if (tilethuhoi.Count == 0)
                                    {
                                        throw new Exception("Chính sách không có tỉ lệ thu hồi !!!");
                                    }
                                    foreach (Entity tlth in tilethuhoi)
                                    {
                                        Money k = new Money(Math.Ceiling((tongtien / sum(chitiethddtm)) * en.GetAttributeValue<decimal>("new_dientichthucte") * tlth.GetAttributeValue<decimal>("new_phantramtilethuhoi") / new decimal(100)));
                                        phanbodautu["new_sotien"] = k;
                                        sotienthat += k.Value;
                                        DateTime dt = ((DateTime)vudautu["new_ngayketthucvuthuhoach"]).ToLocalTime();
                                        phanbodautu["new_hanthanhtoan"] = new DateTime(tlth.GetAttributeValue<int>("new_nam"), dt.Month, dt.Day);
                                        id = service.Create(phanbodautu);

                                        Entity phanbodautu1 = service.Retrieve("new_phanbodautu", id, new ColumnSet(new string[] { "new_maphieuphanbo", "new_name", "new_datra", "new_conlai", "new_sotien" }));
                                        phanbodautu1["new_name"] = "PBĐT" + phanbodautu1["new_maphieuphanbo"].ToString();
                                        phanbodautu1["new_conlai"] = new Money((decimal)((Money)phanbodautu1["new_sotien"]).Value - (decimal)((Money)phanbodautu1["new_datra"]).Value);
                                        DateTime dtStart = vudautu.GetAttributeValue<DateTime>("new_ngaybatdau");
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

                                        if (en.Contains("new_co") == false)
                                        {
                                            en["new_co"] = new Money(0);
                                        }

                                        en["new_co"] = new Money(((Money)phanbodautu1["new_sotien"]).Value + ((Money)en["new_co"]).Value);
                                        service.Update(en);
                                    }
                                }
                                if (sotienthat != tongtien)
                                {
                                    Entity pbdttemp = service.Retrieve("new_phanbodautu", id, new ColumnSet(true));
                                    pbdttemp["new_sotien"] = new Money(((Money)pbdttemp["new_sotien"]).Value - (sotienthat - tongtien));
                                    service.Update(pbdttemp);
                                }
                            }
                            #endregion
                            #region PGN_vattu
                            else if (((OptionSetValue)ct["new_noidunggiaingan"]).Value.ToString() == "100000003") // PGN Vật tư
                            {
                                Entity PGNVT = service.Retrieve("new_phieugiaonhanvattu", ((EntityReference)ct["new_phieugiaonhanvattu"]).Id, new ColumnSet(true));
                                decimal sotienthat = 0;
                                Guid id = new Guid();
                                EntityCollection chitiethddtm = RetrieveNNRecord(service, "new_thuadatcanhtac", "new_phieugiaonhanvattu", "new_new_pgnvattu_new_chitiethddtmia", new ColumnSet(new string[] { "new_khachhang", "new_khachhangdoanhnghiep", "new_hopdongdautumia", "new_dientichthucte", "new_chinhsachdautu", "new_no", "new_co", "new_name" }), "new_phieugiaonhanvattuid", PGNVT.Id);

                                foreach (Entity en in chitiethddtm.Entities)
                                {
                                    if (!en.Contains("new_chinhsachdautu"))
                                    {
                                        throw new Exception(en["new_name"].ToString() + " chưa có chính sách đầu tư");
                                    }
                                    Entity chinhsachdautu = service.Retrieve("new_chinhsachdautu", ((EntityReference)en["new_chinhsachdautu"]).Id, new ColumnSet(true));
                                    List<Entity> tilethuhoi = RetrieveMultiRecord(service, "new_tilethuhoivon", new ColumnSet(new string[] { "new_phantramtilethuhoi", "new_nam" }), "new_chinhsachdautu", chinhsachdautu.Id);

                                    Entity phanbodautu = new Entity("new_phanbodautu");

                                    if (PDNGN.Contains("new_khachhang"))
                                    {
                                        phanbodautu["new_khachhang"] = PDNGN["new_khachhang"];
                                    }
                                    else
                                    {
                                        phanbodautu["new_khachhangdoanhnghiep"] = PDNGN["new_khachhangdoanhnghiep"];
                                    }

                                    if (((OptionSetValue)PDNGN["new_loaihopdong"]).Value.ToString() == "100000000")
                                    {
                                        phanbodautu["new_loaihopdong"] = new OptionSetValue(100000001);
                                        phanbodautu["new_thuacanhtac"] = en.ToEntityReference();
                                    }
                                    else if (((OptionSetValue)PDNGN["new_loaihopdong"]).Value.ToString() == "100000001")
                                    {
                                        phanbodautu["new_loaihopdong"] = new OptionSetValue(100000002);
                                    }
                                    else if (((OptionSetValue)PDNGN["new_loaihopdong"]).Value.ToString() == "100000002")
                                    {
                                        phanbodautu["new_loaihopdong"] = new OptionSetValue(100000004);
                                    }

                                    Entity vudautu = service.Retrieve("new_vudautu", ((EntityReference)PDNGN["new_vudautu"]).Id, new ColumnSet(true));
                                    phanbodautu["new_hopdongdautumia"] = en["new_hopdongdautumia"];
                                    phanbodautu["new_vudautu"] = PDNGN["new_vudautu"];
                                    //phanbodautu["new_phieuchitienmat"] = new EntityReference(target.LogicalName, target.Id);
                                    phanbodautu["new_ngayphatsinh"] = DateTime.Now;
                                    phanbodautu["new_datra"] = new Money(0);
                                    phanbodautu["new_loaidautu"] = ((OptionSetValue)ct["new_loaidautu"]).Value.ToString() == "100000000" ? new OptionSetValue(100000001) : new OptionSetValue(100000000);
                                    if (tilethuhoi.Count == 0)
                                    {
                                        throw new Exception("Chính sách không có tỉ lệ thu hồi !!!");
                                    }
                                    foreach (Entity tlth in tilethuhoi)
                                    {
                                        Money k = new Money(Math.Ceiling((tongtien / sum(chitiethddtm)) * en.GetAttributeValue<decimal>("new_dientichthucte") * tlth.GetAttributeValue<decimal>("new_phantramtilethuhoi") / new decimal(100)));
                                        phanbodautu["new_sotien"] = k;
                                        sotienthat += k.Value;
                                        DateTime dt = ((DateTime)vudautu["new_ngayketthucvuthuhoach"]).ToLocalTime();
                                        phanbodautu["new_hanthanhtoan"] = new DateTime(tlth.GetAttributeValue<int>("new_nam"), dt.Month, dt.Day);
                                        id = service.Create(phanbodautu);

                                        Entity phanbodautu1 = service.Retrieve("new_phanbodautu", id, new ColumnSet(new string[] { "new_maphieuphanbo", "new_name", "new_datra", "new_conlai", "new_sotien" }));
                                        phanbodautu1["new_name"] = "PBĐT" + phanbodautu1["new_maphieuphanbo"].ToString();
                                        phanbodautu1["new_conlai"] = new Money((decimal)((Money)phanbodautu1["new_sotien"]).Value - (decimal)((Money)phanbodautu1["new_datra"]).Value);
                                        DateTime dtStart = vudautu.GetAttributeValue<DateTime>("new_ngaybatdau");
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

                                        if (en.Contains("new_co") == false)
                                        {
                                            en["new_co"] = new Money(0);
                                        }
                                        en["new_co"] = new Money(((Money)phanbodautu1["new_sotien"]).Value + ((Money)en["new_co"]).Value);
                                        service.Update(en);
                                    }
                                }
                                if (sotienthat != tongtien)
                                {
                                    Entity pbdttemp = service.Retrieve("new_phanbodautu", id, new ColumnSet(true));
                                    pbdttemp["new_sotien"] = new Money(((Money)pbdttemp["new_sotien"]).Value - (sotienthat - tongtien));
                                    service.Update(pbdttemp);
                                }
                            }
                            #endregion
                        }
                    }
                    #endregion
                    #region hop dong dau tu thue dat
                    else if (loaihopdong == "100000001") // hop dong dau tu thue dat
                    {
                        List<Entity> ChiTietPDNGN = RetrieveMultiRecord(service, "new_chitietphieudenghigiaingan", new ColumnSet(true), "new_phieudenghigiaingan", PDNGN.Id);
                        decimal sotiendthoanlai = ((Money)PDNGN["new_sotiendthoanlai"]).Value;
                        decimal sotiendtkhonghoanlai = ((Money)PDNGN["new_sotiendtkhonghoanlai"]).Value;

                        Entity hdthuedat = null;
                        if (PDNGN.Contains("new_hopdongdaututhuedat"))
                        {
                            hdthuedat = service.Retrieve("new_hopdongthuedat", ((EntityReference)PDNGN["new_hopdongdaututhuedat"]).Id, new ColumnSet(true));
                        }
                        if (hdthuedat != null)
                        {
                            decimal tongdinhmucdautu = hdthuedat.Contains("new_tongdinhmucdautu") ? ((Money)hdthuedat["new_tongdinhmucdautu"]).Value : 0;
                            decimal tongchi = hdthuedat.Contains("new_tongchi") ? ((Money)hdthuedat["new_tongchi"]).Value : 0;
                            decimal tongsotien = PDNGN.Contains("new_sotien") ? ((Money)PDNGN["new_sotien"]).Value : 0;

                            if (tongsotien + tongchi > tongdinhmucdautu)
                            {
                                throw new Exception("Số tiền giải ngân vượt định mức đầu tư");
                            }
                        }
                    }
                    #endregion
                }
            }
            catch (Exception ex)
            {
                throw new InvalidPluginExecutionException(ex.Message);
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

        Entity RetrieveSingleRecord(IOrganizationService crmservices, string entity, ColumnSet column, string condition, object value)
        {
            QueryExpression q = new QueryExpression(entity);
            q.ColumnSet = column;
            q.Criteria = new FilterExpression();
            q.Criteria.AddCondition(new ConditionExpression(condition, ConditionOperator.Equal, value));
            EntityCollection entc = service.RetrieveMultiple(q);

            return entc.Entities.ToList<Entity>().FirstOrDefault();
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
