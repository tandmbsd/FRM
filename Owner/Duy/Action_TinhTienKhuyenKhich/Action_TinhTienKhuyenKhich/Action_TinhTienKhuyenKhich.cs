using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Action_TinhTienKhuyenKhich
{
    public class Action_TinhTienKhuyenKhich : IPlugin
    {
        IOrganizationService service = null;
        IOrganizationServiceFactory factory = null;
        void IPlugin.Execute(IServiceProvider serviceProvider)
        {
            IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            ITracingService traceService = (ITracingService)serviceProvider.GetService(typeof(ITracingService));

            EntityReference target = (EntityReference)context.InputParameters["Target"];
            if (target.LogicalName == "new_bangketienkhuyenkhich")
            {
                factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
                service = factory.CreateOrganizationService(context.UserId);
                Entity bangketienkk = service.Retrieve(target.LogicalName, target.Id, new ColumnSet(true));

                if (bangketienkk == null)
                {
                    throw new Exception("Bảng kê này không tồn tại !!");
                }

                EntityCollection lstPhieutinhtienkk = RetrieveNNRecord(service, "new_phieutinhtienkhuyenkhich", "new_bangketienkhuyenkhich", "new_new_bangketienkk_new_phieutinhtienkk", new ColumnSet(true), "new_bangketienkhuyenkhichid", bangketienkk.Id);

                if (lstPhieutinhtienkk.Entities.ToList<Entity>().Count > 0)
                {
                    foreach (Entity pttkk in lstPhieutinhtienkk.Entities.ToList<Entity>())
                    {
                        service.Delete(pttkk.LogicalName, pttkk.Id);
                    }
                }

                Entity vudautuhientai = RetrieveMultiRecord(service, "new_vudautu", new ColumnSet(true), "new_danghoatdong", true).FirstOrDefault();

                QueryExpression q = new QueryExpression("new_hopdongdautumia");
                q.ColumnSet = new ColumnSet(true);
                q.Criteria = new FilterExpression(LogicalOperator.And);
                q.Criteria.AddCondition(new ConditionExpression("new_vudautu", ConditionOperator.Equal, vudautuhientai.Id));
                EntityCollection entc = service.RetrieveMultiple(q);

                QueryExpression qChinhsachthuamua = new QueryExpression("new_chinhsachthumua");
                qChinhsachthuamua.ColumnSet = new ColumnSet(true);
                qChinhsachthuamua.Criteria = new FilterExpression(LogicalOperator.And);
                qChinhsachthuamua.Criteria.AddCondition(new ConditionExpression("new_hoatdongapdung", ConditionOperator.Equal, 100000004));
                qChinhsachthuamua.Criteria.AddCondition(new ConditionExpression("new_vudautu", ConditionOperator.Equal, vudautuhientai.Id));
                qChinhsachthuamua.Orders.Add(new OrderExpression("new_thoidiemapdung", OrderType.Descending));
                EntityCollection entcChinhsachthumua = service.RetrieveMultiple(qChinhsachthuamua);

                List<Entity> lstCSTM = entcChinhsachthumua.Entities.ToList<Entity>();
                List<Entity> lsta = new List<Entity>();
                decimal i = new decimal();
                int j = 0;
                EntityReferenceCollection DSPTTKK = new EntityReferenceCollection();

                foreach (Entity hddtm in entc.Entities)
                {
                    j++;
                    Entity phieutinhtienkk = new Entity("new_phieutinhtienkhuyenkhich");
                    phieutinhtienkk["new_name"] = "Phiếu tính tiền KK - " + i;
                    phieutinhtienkk["new_ngaylapphieu"] = DateTime.Now;
                    phieutinhtienkk["new_vudautu"] = vudautuhientai.ToEntityReference();
                    phieutinhtienkk["new_hopdongdautumia"] = hddtm.ToEntityReference();

                    if (hddtm.Contains("new_khachhang"))
                    {
                        phieutinhtienkk["new_khachhang"] = hddtm["new_khachhang"];
                    }
                    else if (hddtm.Contains("new_khachhangdoanhnghiep"))
                    {
                        phieutinhtienkk["new_khachhangdoanhnghiep"] = hddtm["new_khachhangdoanhnghiep"];
                    }

                    Guid phieutinhtienkkId = service.Create(phieutinhtienkk);
                    DSPTTKK.Add(new EntityReference("new_phieutinhtienkhuyenkhich", phieutinhtienkkId));

                    List<Entity> Lstthuadatcanhtac = RetrieveMultiRecord(service, "new_thuadatcanhtac", new ColumnSet(true), "new_hopdongdautumia", hddtm.Id);

                    foreach (Entity chitietHD in Lstthuadatcanhtac)
                    {
                        Entity cstm = new Entity("new_chinhsachthumua");
                        Entity ThuaDat = null;
                        Entity Giongmia = null;
                        if (chitietHD.Contains("new_thuadat"))
                        {
                            ThuaDat = service.Retrieve("new_thuadat", ((EntityReference)chitietHD["new_thuadat"]).Id, new ColumnSet(true));
                        }
                        if (chitietHD.Contains("new_giongmia"))
                        {
                            Giongmia = service.Retrieve("new_giongmia", ((EntityReference)chitietHD["new_giongmia"]).Id, new ColumnSet(true));
                        }

                        #region get chinh sach thu mua
                        foreach (Entity a in entcChinhsachthumua.Entities)
                        {

                            if (a.Contains("new_vutrong_vl"))  // Vu trong
                            {
                                if (chitietHD.Contains("new_vutrong"))
                                {
                                    if (a["new_vutrong_vl"].ToString().IndexOf(((OptionSetValue)chitietHD["new_vutrong"]).Value.ToString()) == -1)
                                        continue;
                                }
                                else
                                    continue;
                            }

                            if (a.Contains("new_loaigocmia_vl"))  // Loai goc mia
                            {
                                if (chitietHD.Contains("new_loaigocmia"))
                                {
                                    if (a["new_loaigocmia_vl"].ToString().IndexOf(((OptionSetValue)chitietHD["new_loaigocmia"]).Value.ToString()) == -1)
                                        continue;
                                }
                                else
                                    continue;
                            }

                            if (a.Contains("new_mucdichsanxuatmia_vl"))  // Muc dich san xuat mia
                            {
                                if (chitietHD.Contains("new_mucdichsanxuatmia"))
                                {
                                    if (a["new_mucdichsanxuatmia_vl"].ToString().IndexOf(((OptionSetValue)chitietHD["new_mucdichsanxuatmia"]).Value.ToString()) == -1)
                                        continue;
                                }
                                else
                                    continue;
                            }

                            if (a.Contains("new_nhomdat_vl"))  // Nhom dat
                            {
                                if (ThuaDat.Attributes.Contains("new_nhomdat"))
                                {
                                    if (a["new_nhomdat_vl"].ToString().IndexOf(((OptionSetValue)ThuaDat["new_nhomdat"]).Value.ToString()) == -1)
                                        continue;
                                }
                                else
                                {
                                    continue;
                                }
                            }

                            if (a.Contains("new_loaisohuudat_vl"))  // Loai chu so huu dat
                            {
                                if (ThuaDat.Attributes.Contains("new_loaisohuudat"))
                                {
                                    if (a["new_loaisohuudat_vl"].ToString().IndexOf(((OptionSetValue)ThuaDat["new_loaisohuudat"]).Value.ToString()) == -1)
                                        continue;
                                }
                                else
                                {
                                    continue;
                                }
                            }


                            if (a.Contains("new_nhomgiongmia_vl"))  // Nhom giong mia
                            {
                                if (Giongmia.Attributes.Contains("new_nhomgiong"))
                                {
                                    if (a["new_nhomgiongmia_vl"].ToString().IndexOf(((OptionSetValue)Giongmia["new_nhomgiong"]).Value.ToString()) == -1)
                                        continue;
                                }
                                else
                                {
                                    continue;
                                }
                            }

                            if (a.Contains("new_tinhtrangmia_vl")) // Tình trạng mía
                            {
                                string vl = "100000000"; // Mia tuoi
                                if (chitietHD.Contains("new_miachay"))
                                    if ((bool)chitietHD["new_miachay"])
                                        vl = "100000001";  // Mia chay
                                if (a["new_tinhtrangmia_vl"].ToString().IndexOf(vl) == -1)
                                    continue;

                                if (vl == "100000001")
                                {
                                    if (a.Contains("new_loaimiachay_vl")) // Loại mía cháy
                                        if (chitietHD.Attributes.Contains("new_loaimiachay_vl"))
                                        {
                                            if (a["new_loaimiachay_vl"].ToString().IndexOf((string)chitietHD["new_loaimiachay_vl"]) == -1)
                                                continue;
                                        }
                                        else
                                        {
                                            continue;
                                        }
                                }
                            }
                            //a
                            // NHom khach hang
                            bool co = false;

                            if (chitietHD.Attributes.Contains("new_khachhang"))
                            {
                                EntityCollection dsNhomKH = RetrieveNNRecord(service, "new_nhomkhachhang", "new_chinhsachthumua", "new_new_chinhsachthumua_new_nhomkhachhang", new ColumnSet(new string[] { "new_nhomkhachhangid" }), "new_chinhsachthumuaid", a.Id);
                                Entity KH = service.Retrieve("contact", ((EntityReference)chitietHD["new_khachhang"]).Id, new ColumnSet(new string[] { "fullname", "new_nhomkhachhang", "new_nangsuatbinhquan" }));

                                if (KH.Attributes.Contains("new_nhomkhachhang"))
                                {
                                    Guid nhomkhId = KH.GetAttributeValue<EntityReference>("new_nhomkhachhang").Id;
                                    Entity nhomKHHDCT = service.Retrieve("new_nhomkhachhang", nhomkhId, new ColumnSet(new string[] { "new_name" }));
                                    if (dsNhomKH != null && dsNhomKH.Entities.Count > 0)
                                    {
                                        foreach (Entity nhomKH in dsNhomKH.Entities)
                                        {
                                            if (nhomKHHDCT.Id == nhomKH.Id)
                                            {
                                                co = true;
                                                break;
                                            }
                                        }
                                    }
                                    else
                                    {
                                        co = true;
                                    }
                                }
                                else   //neu khong co NHomKH trong CTHD
                                {
                                    if (dsNhomKH == null || dsNhomKH.Entities.Count() == 0)
                                    {
                                        co = true;
                                    }
                                }
                            }
                            if (chitietHD.Attributes.Contains("new_khachhangdoanhnghiep"))
                            {
                                EntityCollection dsNhomKH = RetrieveNNRecord(service, "new_nhomkhachhang", "new_chinhsachthumua", "new_new_chinhsachthumua_new_nhomkhachhang", new ColumnSet(new string[] { "new_nhomkhachhangid" }), "new_chinhsachthumuaid", a.Id);
                                Entity KH = service.Retrieve("account", ((EntityReference)chitietHD["new_khachhangdoanhnghiep"]).Id, new ColumnSet(new string[] { "name", "new_nhomkhachhang" }));

                                if (KH.Attributes.Contains("new_nhomkhachhang"))
                                {
                                    Guid nhomkhId = KH.GetAttributeValue<EntityReference>("new_nhomkhachhang").Id;
                                    Entity nhomKHHDCT = service.Retrieve("new_nhomkhachhang", nhomkhId, new ColumnSet(new string[] { "new_name" }));
                                    if (dsNhomKH != null && dsNhomKH.Entities.Count > 0)
                                    {
                                        foreach (Entity nhomKH in dsNhomKH.Entities)
                                        {
                                            if (nhomKHHDCT.Id == nhomKH.Id)
                                            {
                                                co = true;
                                                break;
                                            }
                                        }
                                    }
                                    else
                                    {
                                        co = true;
                                    }
                                }
                                else   //neu khong co NHomKH trong CTHD
                                {
                                    if (dsNhomKH == null || dsNhomKH.Entities.Count() == 0)
                                    {
                                        co = true;
                                    }
                                }
                            }

                            if (co == false)
                                continue;

                            //Vung dia ly
                            co = false;

                            EntityCollection dsVungDL = RetrieveNNRecord(service, "new_vung", "new_chinhsachthumua", "new_new_chinhsachthumua_new_vung", new ColumnSet(new string[] { "new_vungid" }), "new_chinhsachthumuaid", a.Id);

                            if (ThuaDat != null && ThuaDat.Attributes.Contains("new_vungdialy"))
                            {
                                Guid vungdlId = ThuaDat.GetAttributeValue<EntityReference>("new_vungdialy").Id;
                                Entity vungDL = service.Retrieve("new_vung", vungdlId, new ColumnSet(new string[] { "new_name" }));

                                if (dsVungDL != null && dsVungDL.Entities.Count > 0)
                                {
                                    foreach (Entity vungDL1 in dsVungDL.Entities)
                                    {
                                        if (vungDL.Id == vungDL1.Id)
                                        {
                                            co = true;
                                            break;
                                        }
                                    }
                                }
                                else
                                {
                                    co = true;
                                }
                            }
                            else   //neu khong co VungDiaLy trong CTHD
                            {
                                if (dsVungDL == null || dsVungDL.Entities.Count() == 0)
                                {
                                    co = true;
                                }
                            }
                            if (co == false)
                                continue;


                            // Giong mia
                            co = false;

                            EntityCollection dsGiongmia = RetrieveNNRecord(service, "new_giongmia", "new_chinhsachthumua", "new_new_chinhsachthumua_new_giongmia", new ColumnSet(new string[] { "new_giongmiaid" }), "new_chinhsachthumuaid", a.Id);
                            if (dsGiongmia != null && dsGiongmia.Entities.Count > 0)
                            {

                                foreach (Entity giongmia in dsGiongmia.Entities)
                                {
                                    if (Giongmia.Id == giongmia.Id)
                                    {
                                        co = true;
                                        break;
                                    }
                                }
                            }
                            else
                            {
                                co = true;
                            }
                            if (co == false)
                                continue;
                            // Khuyen khich phat trien
                            co = false;
                            EntityCollection dsKKPTHDCT = RetrieveNNRecord(service, "new_khuyenkhichphattrien", "new_thuadatcanhtac", "new_new_chitiethddtmia_new_khuyenkhichpt", new ColumnSet(new string[] { "new_khuyenkhichphattrienid" }), "new_thuadatcanhtacid", chitietHD.Id);
                            EntityCollection dsKKPTCSDT = RetrieveNNRecord(service, "new_khuyenkhichphattrien", "new_chinhsachthumua", "new_new_csthumua_new_khuyenkhichphattrien", new ColumnSet(new string[] { "new_khuyenkhichphattrienid" }), "new_chinhsachthumuaid", a.Id);

                            if (dsKKPTHDCT != null && dsKKPTHDCT.Entities.Count > 0)
                            {
                                if (dsKKPTCSDT != null && dsKKPTCSDT.Entities.Count > 0)
                                {
                                    foreach (Entity kkpt1 in dsKKPTHDCT.Entities)
                                    {
                                        foreach (Entity kkpt2 in dsKKPTCSDT.Entities)
                                        {
                                            //neu tim thay kkpt1 nam trong danh sach dsKKPTCSDT thi thoat khoi for
                                            if (kkpt1.Id == kkpt2.Id)
                                            {
                                                co = true;
                                                break;
                                            }
                                        }
                                        if (co)
                                        {
                                            //thoat vong for thu 1
                                            break;
                                        }
                                    }
                                }
                                else
                                {
                                    co = true;
                                }
                            }
                            else   //neu khong co KKPT trong CTHD
                            {
                                if (dsKKPTCSDT == null || dsKKPTCSDT.Entities.Count() == 0)
                                {
                                    co = true;
                                }
                            }
                            if (co == false)
                                continue;

                            // Nhom cu ly
                            co = false;

                            EntityCollection dsNHomCL = RetrieveNNRecord(service, "new_nhomculy", "new_chinhsachthumua", "new_new_chinhsachthumua_new_nhomculy", new ColumnSet(new string[] { "new_nhomculyid" }), "new_chinhsachthumuaid", a.Id);
                            if (ThuaDat.Attributes.Contains("new_nhomculy"))
                            {
                                Guid nhomclId = ThuaDat.GetAttributeValue<EntityReference>("new_nhomculy").Id;
                                Entity nhomCL = service.Retrieve("new_nhomculy", nhomclId, new ColumnSet(new string[] { "new_name" }));

                                if (dsNHomCL != null && dsNHomCL.Entities.Count > 0)
                                {
                                    foreach (Entity nhomCL1 in dsNHomCL.Entities)
                                    {
                                        if (nhomCL.Id == nhomCL1.Id)
                                        {
                                            co = true;
                                            break;
                                        }
                                    }
                                }
                                else
                                {
                                    co = true;
                                }
                            }
                            else   //neu khong co NHomCL trong CTHD
                            {
                                if (dsNHomCL == null || dsNHomCL.Entities.Count() == 0)
                                {
                                    co = true;
                                }
                            }
                            if (co == false)
                                continue;

                            // Mo hinh khuyen nong

                            co = false;
                            EntityCollection dsMHKN = RetrieveNNRecord(service, "new_mohinhkhuyennong", "new_chinhsachthumua", "new_new_chinhsachthumua_new_mohinhkhuyennon", new ColumnSet(new string[] { "new_mohinhkhuyennongid" }), "new_chinhsachthumuaid", a.Id);

                            if (chitietHD.Attributes.Contains("new_thamgiamohinhkhuyennong"))
                            {
                                Guid mhknId = chitietHD.GetAttributeValue<EntityReference>("new_thamgiamohinhkhuyennong").Id;
                                Entity mhkn = service.Retrieve("new_mohinhkhuyennong", mhknId, new ColumnSet(new string[] { "new_name" }));

                                if (dsMHKN != null && dsMHKN.Entities.Count() > 0)
                                {
                                    foreach (Entity mhkn1 in dsMHKN.Entities)
                                    {
                                        if (mhkn.Id == mhkn1.Id)
                                        {
                                            co = true;
                                            break;
                                        }
                                    }
                                }
                                else
                                {
                                    co = true;
                                }
                            }
                            else   //neu khong co MNKH trong CTHD
                            {
                                if (dsMHKN == null || dsMHKN.Entities.Count() == 0)
                                {
                                    co = true;
                                }
                            }
                            if (co == false)
                                continue;

                            // NHom nang suat

                            co = false;
                            if (chitietHD.Attributes.Contains("new_khachhang"))
                            {
                                EntityCollection dsNhomNS = RetrieveNNRecord(service, "new_nhomnangsuat", "new_chinhsachthumua", "new_new_chinhsachthumua_new_nhomnangsuat", new ColumnSet(new string[] { "new_nhomnangsuatid", "new_nangsuattu", "new_nangsuatden" }), "new_chinhsachthumuaid", a.Id);
                                Entity KH = service.Retrieve("contact", ((EntityReference)chitietHD["new_khachhang"]).Id, new ColumnSet(new string[] { "fullname", "new_nhomkhachhang", "new_nangsuatbinhquan" }));
                                if (KH.Attributes.Contains("new_nangsuatbinhquan"))
                                {
                                    decimal nangsuatbq = KH.GetAttributeValue<decimal>("new_nangsuatbinhquan");
                                    if (dsNhomNS != null && dsNhomNS.Entities.Count() > 0)
                                    {
                                        foreach (Entity nhomns1 in dsNhomNS.Entities)
                                        {
                                            decimal nangsuattu = nhomns1.GetAttributeValue<decimal>("new_nangsuattu");
                                            decimal nangsuatden = nhomns1.GetAttributeValue<decimal>("new_nangsuatden");

                                            if ((nangsuatbq >= nangsuattu) && (nangsuatbq <= nangsuatden))
                                            {
                                                co = true;
                                                break;
                                            }
                                        }
                                    }
                                    else
                                    {
                                        co = true;
                                    }
                                }
                                else
                                {
                                    if (dsNhomNS == null || dsNhomNS.Entities.Count() == 0)
                                    {
                                        co = true;
                                    }
                                }
                            }
                            if (chitietHD.Attributes.Contains("new_khachhangdoanhnghiep"))
                            {
                                Entity khachhang = service.Retrieve("account", ((EntityReference)chitietHD["new_khachhangdoanhnghiep"]).Id, new ColumnSet(new string[] { "name", "new_nangsuatbinhquan" }));
                                EntityCollection dsNhomNS = RetrieveNNRecord(service, "new_nhomnangsuat", "new_chinhsachthumua", "new_new_chinhsachthumua_new_nhomnangsuat", new ColumnSet(new string[] { "new_nhomnangsuatid", "new_nangsuattu", "new_nangsuatden" }), "new_chinhsachthumuaid", a.Id);

                                if (khachhang.Attributes.Contains("new_nangsuatbinhquan"))
                                {
                                    decimal nangsuatbq = khachhang.GetAttributeValue<decimal>("new_nangsuatbinhquan");
                                    if (dsNhomNS != null && dsNhomNS.Entities.Count() > 0)
                                    {
                                        foreach (Entity nhomns1 in dsNhomNS.Entities)
                                        {
                                            decimal nangsuattu = nhomns1.GetAttributeValue<decimal>("new_nangsuattu");
                                            decimal nangsuatden = nhomns1.GetAttributeValue<decimal>("new_nangsuatden");

                                            if ((nangsuatbq >= nangsuattu) && (nangsuatbq <= nangsuatden))
                                            {
                                                co = true;
                                                break;
                                            }
                                        }
                                    }
                                    else
                                    {
                                        co = true;
                                    }
                                }
                                else
                                {
                                    if (dsNhomNS == null || dsNhomNS.Entities.Count() == 0)
                                    {
                                        co = true;
                                    }
                                }
                            }

                            if (co == false)
                                continue;
                            // Mia chay cố ý
                            co = false;

                            EntityCollection BBmiachayCol = FindBBmiachay(service, chitietHD);
                            Entity BBmiachay = new Entity();
                            if (BBmiachayCol != null && BBmiachayCol.Entities.Count() > 0)
                            {
                                BBmiachay = BBmiachayCol.Entities[0];
                                if (a.Contains("new_miachaycoy") && (bool)a["new_miachaycoy"] == true)
                                {
                                    if (BBmiachay.Contains("new_miachaycoy") && (bool)BBmiachay["new_miachaycoy"] == true)
                                        co = true;
                                    else
                                        co = false;
                                }
                                else
                                {
                                    if (BBmiachay.Contains("new_miachaycoy") && (bool)BBmiachay["new_miachaycoy"] == true)
                                        co = false;
                                    else
                                        co = true;
                                }
                            }
                            else
                            {
                                if (a.Contains("new_miachaycoy") && (bool)a["new_miachaycoy"] == true)
                                    co = false;
                                else
                                    co = true;
                            }

                            if (co == false)
                                continue;

                            cstm = a;
                            lsta.Add(a);
                            break;

                        }
                        #endregion

                        if (cstm.Id != null && entcChinhsachthumua.Entities.Count > 0)
                        {
                            i++;

                            Entity chitietphieutinhtienkk = new Entity("new_chitietphieutinhtienkhuyenkhich");
                            chitietphieutinhtienkk["new_name"] = "Chi tiết phiếu tính tiền KK";
                            chitietphieutinhtienkk["new_phieutinhtienkhuyenkhich"] = new EntityReference("new_phieutinhtienkhuyenkhich", phieutinhtienkkId);
                            chitietphieutinhtienkk["new_chitiethddtmia"] = chitietHD.ToEntityReference();
                            chitietphieutinhtienkk["new_chinhsachthumua"] = cstm.ToEntityReference();

                            decimal nangsuatmia = chitietHD.Contains("new_nangsuat") ? (decimal)chitietHD["new_nangsuat"] : 0;
                            decimal sanluongmia = chitietHD.Contains("new_sanluongthucte") ? (decimal)chitietHD["new_sanluongthucte"] : 0;
                            decimal nangsuatduong = chitietHD.Contains("new_nangsuatduong") ? (decimal)chitietHD["new_nangsuatduong"] : 0;

                            decimal dinhmuckkpt = new decimal();
                            decimal dinhmuckknsduongcao = new decimal();

                            List<Entity> lstKKPT = RetrieveMultiRecord(service, "new_chinhsachthumua_khuyenkhichphattrien", new ColumnSet(true), "new_chinhsachthumua", cstm.Id);
                            List<Entity> lstKKNSDuongcao = RetrieveMultiRecord(service, "new_chinhsachthumua_kknangsuatduongcao", new ColumnSet(true), "new_chinhsachthumua", cstm.Id);

                            foreach (Entity kkpt in lstKKPT)
                            {
                                int pheptinhtu = ((OptionSetValue)kkpt["new_phuongthuctinhtu"]).Value;
                                int pheptinhden = ((OptionSetValue)kkpt["new_phuongthuctinhden"]).Value;
                                decimal giatritu = kkpt.GetAttributeValue<decimal>("new_nangsuattu");
                                decimal giatriden = kkpt.GetAttributeValue<decimal>("new_nangsuatden");

                                if (tinhdiem(pheptinhtu, giatritu, pheptinhden, giatriden, nangsuatmia))
                                {
                                    dinhmuckkpt = ((Money)kkpt["new_dinhmuc"]).Value;
                                }
                            }

                            foreach (Entity nsdc in lstKKNSDuongcao)
                            {
                                int pheptinhtu = ((OptionSetValue)nsdc["new_phuongthuctinhtu"]).Value;
                                int pheptinhden = ((OptionSetValue)nsdc["new_phuongthuctinhden"]).Value;
                                decimal giatritu = nsdc.GetAttributeValue<decimal>("new_nangsuattu");
                                decimal giatriden = nsdc.GetAttributeValue<decimal>("new_nangsuatden");

                                if (tinhdiem(pheptinhtu, giatritu, pheptinhden, giatriden, nangsuatduong))
                                {
                                    dinhmuckknsduongcao = ((Money)nsdc["new_dongiakhuyenkhich"]).Value;
                                }
                            }

                            chitietphieutinhtienkk["new_nangsuatduong"] = nangsuatduong;
                            chitietphieutinhtienkk["new_nangsuatmiathucte"] = nangsuatmia;
                            chitietphieutinhtienkk["new_sanluongmia"] = sanluongmia;
                            chitietphieutinhtienkk["new_dinhmuckkpt"] = new Money(dinhmuckkpt);
                            chitietphieutinhtienkk["new_dinhmuckknsduongcao"] = new Money(dinhmuckknsduongcao);
                            chitietphieutinhtienkk["new_tienkkpt"] = new Money(dinhmuckkpt * sanluongmia);
                            chitietphieutinhtienkk["new_tienkknsduongcao"] = new Money(dinhmuckknsduongcao * sanluongmia);

                            service.Create(chitietphieutinhtienkk);
                        }
                    }                    
                }

                service.Associate("new_bangketienkhuyenkhich", target.Id, new Relationship("new_new_bangketienkk_new_phieutinhtienkk"), DSPTTKK);
                context.OutputParameters["ReturnId"] = bangketienkk.Id.ToString();
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

        bool tinhdiem(int pheptinhtu, decimal giatritu, int pheptinhden, decimal giatriden, decimal value)
        {
            bool Fgiatritu = false;
            bool Fgiatriden = false;
            bool ketqua = false;
            switch (pheptinhtu)
            {
                case 100000000: //=
                    if (value == giatritu)
                    {
                        Fgiatritu = true;
                    }
                    break;
                case 100000001: // < 
                    if (value < giatritu)
                    {
                        Fgiatritu = true;
                    }
                    break;
                case 100000002: // >
                    if (value > giatritu)
                    {
                        Fgiatritu = true;
                    }
                    break;
                case 100000003: // <=
                    if (value <= giatritu)
                    {
                        Fgiatritu = true;
                    }
                    break;
                case 100000004: // >=
                    if (value >= giatritu)
                    {
                        Fgiatritu = true;
                    }
                    break;
                default:
                    break;
            }
            switch (pheptinhden)
            {
                case 100000000: //=
                    if (value == giatriden)
                    {
                        Fgiatriden = true;
                    }
                    break;
                case 100000001: // < 
                    if (value < giatriden)
                    {
                        Fgiatriden = true;
                    }
                    break;
                case 100000002: // >
                    if (value > giatriden)
                    {
                        Fgiatriden = true;
                    }
                    break;
                case 100000003: // <=
                    if (value <= giatriden)
                    {
                        Fgiatriden = true;
                    }
                    break;
                case 100000004: // >=
                    if (value >= giatriden)
                    {
                        Fgiatriden = true;
                    }
                    break;
                default:
                    break;
            }

            if (Fgiatritu && Fgiatriden)
            {
                ketqua = true;
            }
            return ketqua;
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
        EntityReferenceCollection RefRetrieveNNRecord(IOrganizationService crmservices, string entity1, string entity2, string relateName, ColumnSet column, string condition, object value)
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
            EntityReferenceCollection RefcollRecords = new EntityReferenceCollection();

            foreach (Entity en in collRecords.Entities)
            {
                RefcollRecords.Add(en.ToEntityReference());
            }

            return RefcollRecords;
        }

        public static EntityCollection FindBBmiachay(IOrganizationService crmservices, Entity chitietHD)
        {
            string fetchXml =
              @"<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='false'>
                  <entity name='new_bienbanmiachay'>
                    <attribute name='new_name' />
                    <attribute name='new_nhanvienlapbienban' />
                    <attribute name='new_ngaylapbienban' />
                    <attribute name='new_ngaygiochay' />
                    <attribute name='new_khachhang' />
                    <attribute name='new_dientichchay' />
                    <attribute name='new_miachaycoy' />
                    <attribute name='createdon' />
                    <attribute name='new_sobienban' />
                    <attribute name='new_bienbanmiachayid' />
                    <order attribute='createdon' descending='true' />
                    <filter type='and'>
                      <condition attribute='new_thuadatcanhtac' operator='eq' uitype='new_thuadatcanhtac' value='{0}' />
                      <condition attribute='new_trangthai' operator='eq' value='100000002' />
                    </filter>
                  </entity>
                </fetch>";

            fetchXml = string.Format(fetchXml, chitietHD.Id);
            EntityCollection entc = crmservices.RetrieveMultiple(new FetchExpression(fetchXml));

            return entc;
        }

    }
}
