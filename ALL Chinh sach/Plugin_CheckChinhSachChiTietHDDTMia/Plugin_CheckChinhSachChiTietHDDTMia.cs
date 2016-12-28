using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;


namespace Plugin_CheckChinhSachChiTietHDDTMia
{
    public class Plugin_CheckChinhSachChiTietHDDTMia : IPlugin
    {
        // moi nhat
        IOrganizationService service = null;
        IOrganizationServiceFactory factory = null;

        void IPlugin.Execute(IServiceProvider serviceProvider)
        {
            IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
            service = factory.CreateOrganizationService(context.UserId);
            ITracingService traceService = (ITracingService)serviceProvider.GetService(typeof(ITracingService));
            Entity target = null;

            bool co = false;

            if (context.MessageName == "Create")
            {
                target = (Entity)context.InputParameters["Target"];

                if (target.Contains("new_phuluchopdongid"))
                    co = false;
                else
                    co = true;
            }
            else if (context.MessageName == "Update")
            {F
                target = (Entity)context.InputParameters["Target"];
                co = CheckRunUpdate(target);
            }
            else if (context.MessageName == "Associate" || context.MessageName == "Disassociate")
            {
                string relationshipName = "";
                if (context.InputParameters.Contains("Relationship"))
                    relationshipName = context.InputParameters["Relationship"].ToString();
                if (relationshipName.Trim().ToLower() == "new_new_chitiethddtmia_new_khuyenkhichpt.")
                {
                    co = true;
                }
                if (co)
                {
                    if (context.InputParameters.Contains("Target") && context.InputParameters["Target"] is EntityReference)
                    {
                        EntityReference targetEntity = (EntityReference)context.InputParameters["Target"];
                        target = new Entity(targetEntity.LogicalName);
                        target.Id = targetEntity.Id;
                    }
                }
            }

            if (co)
            {
                traceService.Trace("bat dau chay");
                Entity CSDautu = null;
                Entity CSTamUng = null;
                Entity CSThamCanh = null;

                Entity CTHDMia = service.Retrieve("new_thuadatcanhtac", target.Id, new ColumnSet(new string[] {
                    "new_hopdongdautumia","new_giongmia","new_thuadat","new_khachhang","new_khachhangdoanhnghiep","createdon","new_vutrong","new_mucdichsanxuatmia",
                    "new_loaisohuudat", "new_loaigocmia","new_luugoc","new_thamgiamohinhkhuyennong","new_dientichhopdong", "new_nhomculy"  }));
                if (!CTHDMia.Contains("new_hopdongdautumia")) throw new Exception("Chi tiết HĐĐT mía chưa gắn với hợp đồng ĐT mía !");
                Entity HDDTMia = service.Retrieve("new_hopdongdautumia", ((EntityReference)CTHDMia["new_hopdongdautumia"]).Id, new ColumnSet(new string[] { "new_vudautu", "new_chinhantienmat" }));
                if (!CTHDMia.Contains("new_giongmia")) throw new Exception("Chi tiết HĐĐT mía chưa có thông tin giống mía dự kiến !");
                Entity GiongMia = service.Retrieve("new_giongmia", ((EntityReference)CTHDMia["new_giongmia"]).Id, new ColumnSet(new string[] { "new_name", "new_nhomgiong" }));
                if (!GiongMia.Contains("new_nhomgiong")) throw new Exception("Giống mía " + GiongMia["new_name"].ToString() + " chưa có thông tin nhóm giống !");
                if (!CTHDMia.Contains("new_thuadat")) throw new Exception("Chi tiết HĐĐT mía chưa có thông tin thửa đất !");
                Entity ThuaDat = service.Retrieve("new_thuadat", ((EntityReference)CTHDMia["new_thuadat"]).Id, new ColumnSet(new string[] { "new_name", "new_diachi", "new_nhomdat" }));
                if (!ThuaDat.Contains("new_nhomdat")) throw new Exception("Thửa đất " + ThuaDat["new_name"].ToString() + " chưa có thông tin nhóm đất !");
                if (!ThuaDat.Contains("new_diachi")) throw new Exception("Thửa đất " + ThuaDat["new_name"].ToString() + " chưa có thông tin địa chỉ !");
                if (!CTHDMia.Contains("new_khachhang") && !CTHDMia.Contains("new_khachhangdoanhnghiep")) throw new Exception("Thửa đất " + ThuaDat["new_name"].ToString() + " chưa có thông tin khách hàng !");
                Entity KhachHang = service.Retrieve(CTHDMia.Contains("new_khachhang") ? "contact" : "account",
                    (CTHDMia.Contains("new_khachhang") ? ((EntityReference)CTHDMia["new_khachhang"]).Id : ((EntityReference)CTHDMia["new_khachhangdoanhnghiep"]).Id),
                    new ColumnSet(new string[] { "new_nhomkhachhang", "new_nangsuatbinhquan" }));

                EntityCollection CSThoa = GetCSDTFromHDMia(service, "new_chinhsachdautu", "new_hopdongdautumia",
                    "new_new_chinhsachdautu_new_hopdongdautumia", new ColumnSet(new string[] { "new_ngayapdung", "new_thutuuutien",
                        "new_cantrutoithieu", "new_dinhmucdautukhonghoanlai", "new_dinhmuctamung", "new_dinhmucphanbontoithieu" }),
                    "new_hopdongdautumiaid", HDDTMia.Id, "new_ngayapdung", ((EntityReference)HDDTMia["new_vudautu"]).Id);

                if (CSThoa.Entities.Count > 0)
                {
                    traceService.Trace("co csdt thoa " + CSThoa.Entities.Count);

                    DateTime maxdate0 = new DateTime(1, 1, 1);
                    DateTime maxdate1 = new DateTime(1, 1, 1);
                    DateTime maxdate4 = new DateTime(1, 1, 1);

                    traceService.Trace("maxdate ");

                    foreach (Entity m in CSThoa.Entities)
                    {
                        traceService.Trace("csdt co muc dich dt " + m.Id);

                        if (((OptionSetValue)m["new_mucdichdautu"]).Value == 100000000)    // new_mucdichdautu
                        {
                            traceService.Trace("csdt co muc dich dt Trong mia");
                            if ((DateTime)m["new_ngayapdung"] >= maxdate0)
                                CSDautu = m;
                        }
                        else if (((OptionSetValue)m["new_mucdichdautu"]).Value == 100000001)
                        {
                            traceService.Trace("csdt co muc dich Tham canh  " + m["new_mucdichdautu"].ToString());
                            if ((DateTime)m["new_ngayapdung"] >= maxdate1)
                                CSThamCanh = m;
                        }
                        else if (((OptionSetValue)m["new_mucdichdautu"]).Value == 100000004)
                        {
                            if ((DateTime)m["new_ngayapdung"] >= maxdate4)
                                CSTamUng = m;
                        }
                    }
                }

                if (CSDautu == null || CSTamUng == null || CSThamCanh == null)
                {
                    StringBuilder qChinhSach = new StringBuilder();
                    qChinhSach.AppendFormat("<fetch mapping='logical' version='1.0' no-lock='true'>");
                    qChinhSach.AppendFormat("<entity name='new_chinhsachdautu'>");
                    qChinhSach.AppendFormat("<attribute name='{0}' />", "new_chinhsachdautuid");
                    qChinhSach.AppendFormat("<attribute name='{0}' />", "new_name");
                    qChinhSach.AppendFormat("<attribute name='{0}' />", "new_ngayapdung");
                    qChinhSach.AppendFormat("<attribute name='{0}' />", "new_thutuuutien");
                    qChinhSach.AppendFormat("<attribute name='{0}' />", "new_cantrutoithieu");
                    qChinhSach.AppendFormat("<attribute name='{0}' />", "new_dinhmucdautuhoanlai");
                    qChinhSach.AppendFormat("<attribute name='{0}' />", "new_dinhmucdautukhonghoanlai");
                    qChinhSach.AppendFormat("<attribute name='{0}' />", "new_dinhmuctamung");
                    qChinhSach.AppendFormat("<attribute name='{0}' />", "new_dinhmucphanbontoithieu");
                    qChinhSach.AppendFormat("<attribute name='{0}' />", "new_mucdichdautu");
                    qChinhSach.AppendFormat("<attribute name='{0}' />", "new_loailaisuatcodinhthaydoi");
                    //qChinhSach.AppendFormat("<attribute name='{0}' />", "new_muclaisuatdautu");

                    qChinhSach.AppendFormat("<filter type='and'>");
                    qChinhSach.AppendFormat("<condition attribute='new_vudautu' operator='eq' value='{0}' />", ((EntityReference)HDDTMia["new_vudautu"]).Id.ToString());
                    qChinhSach.AppendFormat("<condition attribute='statecode' operator='eq' value='{0}' />", 0);
                    qChinhSach.AppendFormat("<condition attribute='new_ngayapdung' operator='le' value='{0}' />", ((DateTime)CTHDMia["createdon"]).AddHours(7).ToString("yyyy/MM/dd HH:mm:ss"));
                    qChinhSach.AppendFormat("<condition attribute='new_mucdichdautu' operator='in' >");
                    if (CSDautu == null) qChinhSach.AppendFormat("   <value>100000000</value>");
                    if (CSThamCanh == null) qChinhSach.AppendFormat("   <value>100000001</value>");
                    if (CSTamUng == null) qChinhSach.AppendFormat("   <value>100000004</value>");
                    qChinhSach.AppendFormat("</condition>");

                    if (CTHDMia.Contains("new_vutrong"))
                    {
                        qChinhSach.AppendFormat("<filter type='or'>");
                        qChinhSach.AppendFormat("     <condition attribute='new_vutrong_vl' operator='like' value='%{0}%' />", ((OptionSetValue)CTHDMia["new_vutrong"]).Value);
                        qChinhSach.AppendFormat("     <condition attribute='new_vutrong_vl' operator='null' />");
                        qChinhSach.AppendFormat("</filter>");
                    }
                    else
                        qChinhSach.AppendFormat("<condition attribute='new_vutrong_vl' operator='null' />");

                    if (CTHDMia.Contains("new_mucdichsanxuatmia"))
                    {
                        qChinhSach.AppendFormat("<filter type='or'>");
                        qChinhSach.AppendFormat("     <condition attribute='new_mucdichsanxuatmia_vl' operator='like' value='%{0}%' />", ((OptionSetValue)CTHDMia["new_mucdichsanxuatmia"]).Value);
                        qChinhSach.AppendFormat("     <condition attribute='new_mucdichsanxuatmia_vl' operator='null' />");
                        qChinhSach.AppendFormat("</filter>");
                    }
                    else qChinhSach.AppendFormat("<condition attribute='new_mucdichsanxuatmia_vl' operator='null' />");

                    if (CTHDMia.Contains("new_loaisohuudat"))
                    {
                        qChinhSach.AppendFormat("<filter type='or'>");
                        qChinhSach.AppendFormat("     <condition attribute='new_loaisohuu_vl' operator='like' value='%{0}%' />", ((OptionSetValue)CTHDMia["new_loaisohuudat"]).Value);
                        qChinhSach.AppendFormat("     <condition attribute='new_loaisohuu_vl' operator='null' />");
                        qChinhSach.AppendFormat("</filter>");
                    }
                    else qChinhSach.AppendFormat("<condition attribute='new_loaisohuu_vl' operator='null' />");

                    if (CTHDMia.Contains("new_loaigocmia"))
                    {
                        qChinhSach.AppendFormat("<filter type='or'>");
                        qChinhSach.AppendFormat("     <condition attribute='new_loaigocmia_vl' operator='like' value='%{0}%' />", ((OptionSetValue)CTHDMia["new_loaigocmia"]).Value);
                        qChinhSach.AppendFormat("     <condition attribute='new_loaigocmia_vl' operator='null' />");
                        qChinhSach.AppendFormat("</filter>");

                        if (((OptionSetValue)CTHDMia["new_loaigocmia"]).Value == 100000001)
                        {
                            if (CTHDMia.Contains("new_luugoc"))
                            {
                                qChinhSach.AppendFormat("<filter type='or'>");
                                qChinhSach.AppendFormat("     <condition attribute='new_luugoc' operator='eq' value='{0}' />", ((OptionSetValue)CTHDMia["new_luugoc"]).Value);
                                qChinhSach.AppendFormat("     <condition attribute='new_luugoc' operator='null' />");
                                qChinhSach.AppendFormat("</filter>");
                            }
                            else qChinhSach.AppendFormat("<condition attribute='new_luugoc' operator='null' />");
                        }
                    }
                    else qChinhSach.AppendFormat("<condition attribute='new_loaigocmia_vl' operator='null' />");

                    if (CTHDMia.Contains("new_giongmia"))
                    {
                        qChinhSach.AppendFormat("<filter type='or'>");
                        qChinhSach.AppendFormat("     <condition attribute='new_nhomgiongmia_vl' operator='like' value='%{0}%' />", ((OptionSetValue)GiongMia["new_nhomgiong"]).Value);
                        qChinhSach.AppendFormat("     <condition attribute='new_nhomgiongmia_vl' operator='null' />");
                        qChinhSach.AppendFormat("</filter>");
                    }
                    else qChinhSach.AppendFormat("<condition attribute='new_nhomgiongmia_vl' operator='null' />");

                    if (CTHDMia.Contains("new_thuadat"))
                    {
                        qChinhSach.AppendFormat("<filter type='or'>");
                        qChinhSach.AppendFormat("     <condition attribute='new_nhomdat_vl' operator='like' value='%{0}%' />", ((OptionSetValue)ThuaDat["new_nhomdat"]).Value);
                        qChinhSach.AppendFormat("     <condition attribute='new_nhomdat_vl' operator='null' />");
                        qChinhSach.AppendFormat("</filter>");
                    }
                    else qChinhSach.AppendFormat("<condition attribute='new_nhomdat_vl' operator='null' />");

                    qChinhSach.AppendFormat("</filter>");
                    qChinhSach.AppendFormat("</entity>");
                    qChinhSach.AppendFormat("</fetch>");

                    EntityCollection result = service.RetrieveMultiple(new FetchExpression(qChinhSach.ToString()));
                    traceService.Trace("Số lượng chính sach tìm được trước khi kiểm tra dk : " + result.Entities.Count.ToString());
                    if (result.Entities.Count <= 0 && CSDautu == null)
                        throw new Exception("Không tìm thấy chính sách nào phù hợp !");
                    else
                    {
                        for (int i = 0; i < result.Entities.Count; i++)
                        {
                            #region check chinh sach
                            Entity cs = result.Entities[i];
                            traceService.Trace("Check " + cs["new_name"].ToString());

                            EntityCollection csNhomKH = RetrieveNNRecord(service, "new_nhomkhachhang", "new_chinhsachdautu", "new_new_chinhsachdautu_new_nhomkhachhang", new ColumnSet(new string[] { "new_nhomkhachhangid" }), "new_chinhsachdautuid", cs.Id);
                            if (csNhomKH.Entities.Count > 0)
                            {
                                if (!KhachHang.Contains("new_nhomkhachhang")) throw new Exception("Khách hàng trên thửa đất " + ThuaDat["new_name"].ToString() + "chưa có thông tin nhóm khách hàng !");
                                bool check = false;
                                foreach (Entity t in csNhomKH.Entities)
                                    if (t.Id == ((EntityReference)KhachHang["new_nhomkhachhang"]).Id)
                                    {
                                        check = true;
                                        break;
                                    }
                                if (!check)
                                {
                                    result.Entities.RemoveAt(i);
                                    i--;
                                    continue;
                                }
                            }
                            traceService.Trace("Pass nhom khách hàng");

                            EntityCollection csVungDL = RetrieveNNRecord(service, "new_vung", "new_chinhsachdautu", "new_new_chinhsachdautu_new_vung", new ColumnSet(new string[] { "new_vungid" }), "new_chinhsachdautuid", cs.Id);
                            traceService.Trace(csVungDL.Entities.Count.ToString());
                            if (csVungDL.Entities.Count > 0)
                            {
                                bool check = false;
                                List<Guid> dsvung = new List<Guid>();
                                foreach (Entity n in csVungDL.Entities)
                                    dsvung.Add(n.Id);

                                Entity diachi = service.Retrieve("new_diachi", ((EntityReference)ThuaDat["new_diachi"]).Id,
                                    new ColumnSet(new string[] { "new_path" }));
                                QueryExpression qe = new QueryExpression("new_vungdialy_hanhchinh");
                                qe.NoLock = true;
                                qe.ColumnSet = new ColumnSet(new string[] { "new_vungdialy_hanhchinhid", "new_vungdialy", "new_path" });
                                qe.Criteria.AddCondition(new ConditionExpression("new_vungdialy", ConditionOperator.In, dsvung.ToArray()));
                                //traceService.Trace(diachi["new_path"].ToString());
                                foreach (Entity n in service.RetrieveMultiple(qe).Entities)
                                {
                                    //traceService.Trace(n["new_path"].ToString() + " -" + diachi["new_path"].ToString().Contains(n["new_path"].ToString()));
                                    if (!diachi.Contains("new_path"))
                                        continue;
                                    
                                    if (diachi["new_path"].ToString().Contains(n["new_path"].ToString()))
                                    {
                                        check = true;
                                        break;
                                    }
                                }

                                if (!check)
                                {
                                    result.Entities.RemoveAt(i);
                                    i--;
                                    continue;
                                }
                            }
                            traceService.Trace("Pass vùng dia li");

                            EntityCollection csNhomNangSuat = RetrieveNNRecord(service, "new_nhomnangsuat", "new_chinhsachdautu", "new_new_chinhsachdautu_new_nhomnangsuat", new ColumnSet(new string[] { "new_name", "new_nangsuattu", "new_nangsuatden" }), "new_chinhsachdautuid", cs.Id);
                            if (csNhomNangSuat.Entities.Count > 0)
                            {
                                decimal nsbinhquan = (KhachHang.Contains("new_nangsuatbinhquan") ? (decimal)KhachHang["new_nangsuatbinhquan"] : 0);
                                bool check = false;
                                foreach (Entity t in csNhomNangSuat.Entities)
                                    if (t.Contains("new_nangsuattu") && t.Contains("new_nangsuatden"))
                                    {
                                        if (nsbinhquan >= (decimal)t["new_nangsuattu"] && nsbinhquan <= (decimal)t["new_nangsuatden"])
                                        {
                                            check = true;
                                            break;
                                        }
                                    }
                                    else throw new Exception("Thông tin Nhóm năng suất '" + t["new_name"].ToString() + " thiếu thông tin năng suất từ - đến !");

                                if (!check)
                                {
                                    result.Entities.RemoveAt(i);
                                    i--;
                                    continue;
                                }
                            }

                            traceService.Trace("Pass nhom năng suất");

                            EntityCollection csGiong = RetrieveNNRecord(service, "new_giongmia", "new_chinhsachdautu", "new_new_chinhsachdautu_new_giongmia", new ColumnSet(new string[] { "new_giongmiaid" }), "new_chinhsachdautuid", cs.Id);
                            if (csGiong.Entities.Count > 0)
                            {
                                bool check = false;
                                foreach (Entity t in csGiong.Entities)
                                    if (t.Id == ((EntityReference)CTHDMia["new_giongmia"]).Id)
                                    {
                                        check = true;
                                        break;
                                    }
                                if (!check)
                                {
                                    result.Entities.RemoveAt(i);
                                    i--;
                                    continue;
                                }
                            }

                            traceService.Trace("Pass giống mía");

                            EntityCollection csNHomCL = RetrieveNNRecord(service, "new_nhomculy", "new_chinhsachdautu", "new_new_chinhsachdautu_new_nhomculy", new ColumnSet(new string[] { "new_nhomculyid" }), "new_chinhsachdautuid", cs.Id);
                            if (csNHomCL.Entities.Count > 0)
                            {
                                if (!CTHDMia.Contains("new_nhomculy")) throw new Exception("Chi tiết HD mía trên thửa đất " + ThuaDat["new_name"].ToString() + " chưa có thông tin về nhóm cự ly!");
                                bool check = false;
                                foreach (Entity t in csNHomCL.Entities)
                                    if (t.Id == ((EntityReference)CTHDMia["new_nhomculy"]).Id)
                                    {
                                        check = true;
                                        break;
                                    }
                                if (!check)
                                {
                                    result.Entities.RemoveAt(i);
                                    i--;
                                    continue;
                                }
                            }

                            traceService.Trace("Pass nhom cự li");

                            EntityCollection csKKPT = RetrieveNNRecord(service, "new_khuyenkhichphattrien", "new_chinhsachdautu", "new_new_chinhsachdautu_new_khuyenkhichphatt", new ColumnSet(new string[] { "new_khuyenkhichphattrienid" }), "new_chinhsachdautuid", cs.Id);
                            if (csKKPT.Entities.Count > 0)
                            {
                                EntityCollection dsKKPTThuaDat = RetrieveNNRecord(service, "new_khuyenkhichphattrien", "new_thuadatcanhtac", "new_new_chitiethddtmia_new_khuyenkhichpt", new ColumnSet(new string[] { "new_khuyenkhichphattrienid" }), "new_thuadatcanhtacid", CTHDMia.Id);
                                bool check = false;
                                if (dsKKPTThuaDat.Entities.Count > 0)
                                {
                                    foreach (Entity t in csKKPT.Entities)
                                    {
                                        foreach (Entity t2 in dsKKPTThuaDat.Entities)
                                            if (t.Id == t2.Id)
                                            {
                                                check = true;
                                                break;
                                            }
                                        if (check) break;
                                    }
                                }
                                if (!check)
                                {
                                    result.Entities.RemoveAt(i);
                                    i--;
                                    continue;
                                }
                            }

                            traceService.Trace("Pass KKPT");

                            EntityCollection csKhuyenNong = RetrieveNNRecord(service, "new_mohinhkhuyennong", "new_chinhsachdautu", "new_new_chinhsachdautu_new_mohinhkhuyennong", new ColumnSet(new string[] { "new_mohinhkhuyennongid" }), "new_chinhsachdautuid", cs.Id);
                            if (csKhuyenNong.Entities.Count > 0)
                            {
                                bool check = false;
                                if (CTHDMia.Contains("new_thamgiamohinhkhuyennong"))
                                {
                                    foreach (Entity t in csKhuyenNong.Entities)
                                        if (t.Id == ((EntityReference)CTHDMia["new_thamgiamohinhkhuyennong"]).Id)
                                        {
                                            check = true;
                                            break;
                                        }
                                }
                                if (!check)
                                {
                                    result.Entities.RemoveAt(i);
                                    i--;
                                    continue;
                                }
                            }

                            traceService.Trace("Pass mô hình khuyến nông");

                            #endregion
                        }

                        DateTime maxdate0 = new DateTime(1, 1, 1);
                        DateTime maxdate1 = new DateTime(1, 1, 1);
                        DateTime maxdate4 = new DateTime(1, 1, 1);
                        //throw new Exception(((OptionSetValue)result[0]["new_mucdichdautu"]).Value.ToString());
                        foreach (Entity m in result.Entities)
                        {
                            if (((OptionSetValue)m["new_mucdichdautu"]).Value == 100000000) // cs dau tu 
                            {
                                if ((DateTime)m["new_ngayapdung"] >= maxdate0)
                                    CSDautu = m;
                            }
                            else if (((OptionSetValue)m["new_mucdichdautu"]).Value == 100000001)//CSDautu tham thanh
                            {
                                if ((DateTime)m["new_ngayapdung"] >= maxdate1)
                                    CSThamCanh = m;
                            }
                            else if (((OptionSetValue)m["new_mucdichdautu"]).Value == 100000004)//cs tam ứng
                            {
                                if ((DateTime)m["new_ngayapdung"] >= maxdate4)
                                    CSTamUng = m;
                            }
                        }
                    }
                }

                if (CSDautu == null) throw new Exception("Không tìm thấy chính sách nào phù hợp !");
                else
                {
                    if (context.MessageName == "Update" || context.MessageName == "Associate" || context.MessageName == "Disassociate")
                    {
                        EntityCollection csBSCu = RetrieveNNRecord(service, "new_chinhsachdautuchitiet", "new_thuadatcanhtac", "new_new_thuadatcanhtac_new_chinhsachdautuct", new ColumnSet("new_chinhsachdautuchitietid"), "new_thuadatcanhtacid", CTHDMia.Id);
                        EntityReferenceCollection csBSRef = new EntityReferenceCollection();
                        foreach (Entity a in csBSCu.Entities)
                            csBSRef.Add(a.ToEntityReference());
                        if (csBSCu.Entities.Count > 0)
                        {
                            service.Disassociate("new_thuadatcanhtac", CTHDMia.Id, new Relationship("new_new_thuadatcanhtac_new_chinhsachdautuct"), csBSRef);
                        }
                    }

                    string fetchTSVDT =
                                     @"<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='false'>
                                  <entity name='new_thongsotheovudautu'>
                                    <attribute name='new_name' />
                                    <attribute name='createdon' />
                                    <attribute name='new_vudautu' />
                                    <attribute name='new_loai' />
                                    <attribute name='new_giatri' />
                                    <attribute name='new_giatien' />
                                    <attribute name='new_apdungtu' />
                                    <attribute name='new_thongsotheovudautuid' />
                                    <order attribute='new_name' descending='false' />
                                    <filter type='and'>
                                      <condition attribute='statecode' operator='eq' value='0' />
                                      <condition attribute='new_vudautu' operator='eq' uitype='new_vudautu' value='{0}' />
                                      <condition attribute='new_apdungtu' operator='le' value='{1}' />
                                    </filter>
                                  </entity>
                                </fetch>";
                    fetchTSVDT = string.Format(fetchTSVDT, ((EntityReference)HDDTMia["new_vudautu"]).Id, ((DateTime)CTHDMia["createdon"]).AddHours(7).ToString("yyyy/MM/dd HH:mm:ss"));
                    EntityCollection dsTSVDT = service.RetrieveMultiple(new FetchExpression(fetchTSVDT));

                    //set chính sách
                    Entity up = new Entity(CTHDMia.LogicalName);
                    up.Id = CTHDMia.Id;
                    up["new_chinhsachdautu"] = CSDautu.ToEntityReference();
                    if (CSTamUng != null) up["new_chinhsachtamung"] = CSTamUng.ToEntityReference();
                    else up["new_chinhsachtamung"] = null;
                    if (CSThamCanh != null) up["new_chinhsachthamcanh"] = CSThamCanh.ToEntityReference();
                    else up["new_chinhsachthamcanh"] = null;

                    if (CSDautu.Contains("new_loailaisuatcodinhthaydoi"))
                        up["new_loailaisuat"] = ((bool)CSDautu["new_loailaisuatcodinhthaydoi"] == true ? new OptionSetValue(100000000) : new OptionSetValue(100000001));
                    else up["new_loailaisuat"] = null;
                    if (CSDautu.Contains("new_muclaisuatdautu"))
                        up["new_laisuat"] = CSDautu["new_muclaisuatdautu"];
                    else
                        foreach (Entity TSVDT in dsTSVDT.Entities)
                            if (TSVDT.GetAttributeValue<OptionSetValue>("new_loai").Value == 100000001)
                                if (TSVDT.Attributes.Contains("new_giatri"))
                                {
                                    decimal mucls = (TSVDT.Contains("new_giatri") ? TSVDT.GetAttributeValue<decimal>("new_giatri") : 0);
                                    up["new_laisuat"] = mucls;
                                    break;
                                }
                    if (!up.Contains("new_laisuat")) up["new_laisuat"] = null;

                    if (CSDautu.Contains("new_cachtinhlai"))
                        up["new_cachtinhlai"] = CSDautu["new_cachtinhlai"];
                    else up["new_cachtinhlai"] = null; traceService.Trace("new_cachtinhlai null");

                    foreach (Entity TSVDT in dsTSVDT.Entities)
                        if (TSVDT.GetAttributeValue<OptionSetValue>("new_loai").Value == 100000004)
                            if (TSVDT.Attributes.Contains("new_giatien"))
                            {
                                Money giamiadk = TSVDT.GetAttributeValue<Money>("new_giatien");
                                up["new_giamiadukien"] = giamiadk;
                                traceService.Trace("new_giamiadukien");
                                break;
                            }
                    if (!up.Contains("new_giamiadukien"))
                        up["new_giamiadukien"] = null;
                    traceService.Trace("new_giamiadukien null");

                    // Set thu tu uu tien
                    if (CSDautu.Contains("new_thutuuutien"))
                    {
                        EntityReference ttutRef = CSDautu.GetAttributeValue<EntityReference>("new_thutuuutien");
                        up["new_thutuuutien"] = ttutRef;
                    }

                    // Set ty le can tru toi thieu

                    up["new_cantrutoithieu"] = (CSDautu.Contains("new_cantrutoithieu") ? CSDautu["new_cantrutoithieu"] : new decimal(0));
                    traceService.Trace("new_cantrutoithieu");

                    #region Get Chinh Sach Bo Sung
                    traceService.Trace("start get chinh sach bo sung");
                    traceService.Trace(((DateTime)CTHDMia["createdon"]).ToString());
                    StringBuilder qCSBS = new StringBuilder();
                    qCSBS.AppendFormat("<fetch mapping='logical' version='1.0' no-lock='true'>");
                    qCSBS.AppendFormat("<entity name='new_chinhsachdautuchitiet'>");
                    qCSBS.AppendFormat("<attribute name='{0}' />", "new_sotienbosung_khl");
                    qCSBS.AppendFormat("<attribute name='{0}' />", "new_sotienbosung");
                    qCSBS.AppendFormat("<attribute name='{0}' />", "new_machinhsach");
                    qCSBS.AppendFormat("<attribute name='{0}' />", "new_bosungphanbon");
                    qCSBS.AppendFormat("<attribute name='{0}' />", "new_bosungtienmat");
                    qCSBS.AppendFormat("<attribute name='{0}' />", "new_nhomnangsuat");
                    qCSBS.AppendFormat("<attribute name='{0}' />", "new_khuyenkhichphattrien");
                    qCSBS.AppendFormat("<filter type='and'>");
                    qCSBS.AppendFormat("<condition attribute='new_vudautu' operator='eq' value='{0}' />", ((EntityReference)HDDTMia["new_vudautu"]).Id.ToString());
                    qCSBS.AppendFormat("<condition attribute='statecode' operator='eq' value='{0}' />", 0);
                    qCSBS.AppendFormat("<condition attribute='new_nghiemthu' operator='eq' value='0' />");
                    qCSBS.AppendFormat("<condition attribute='new_tungay' operator='le' value='{0}' />", ((DateTime)CTHDMia["createdon"]).AddHours(7).ToString("yyyy/MM/dd HH:mm:ss"));
                    qCSBS.AppendFormat("<condition attribute='new_denngay' operator='ge' value='{0}' />", ((DateTime)CTHDMia["createdon"]).AddHours(7).ToString("yyyy/MM/dd HH:mm:ss"));
                    traceService.Trace(((DateTime)CTHDMia["createdon"]).AddHours(7).ToString("yyyy/MM/dd HH:mm:ss"));
                    if (CTHDMia.Contains("new_loaisohuudat"))
                    {
                        qCSBS.AppendFormat("<filter type='or'>");
                        qCSBS.AppendFormat("     <condition attribute='new_loaisohuudat' operator='eq' value='{0}' />", ((OptionSetValue)CTHDMia["new_loaisohuudat"]).Value);
                        qCSBS.AppendFormat("     <condition attribute='new_loaisohuudat' operator='null' />");
                        qCSBS.AppendFormat("</filter>");
                    }
                    else qCSBS.AppendFormat("<condition attribute='new_loaisohuudat' operator='null' />");

                    if (CTHDMia.Contains("new_giongmia"))
                    {
                        qCSBS.AppendFormat("<filter type='or'>");
                        qCSBS.AppendFormat("     <condition attribute='new_giongmia' operator='eq' value='{0}' />", ((EntityReference)CTHDMia["new_giongmia"]).Id);
                        qCSBS.AppendFormat("     <condition attribute='new_giongmia' operator='null' />");
                        qCSBS.AppendFormat("</filter>");
                    }
                    else qCSBS.AppendFormat("<condition attribute='new_giongmia' operator='null' />");

                    if (CTHDMia.Contains("new_loaigocmia"))
                    {
                        qCSBS.AppendFormat("<filter type='or'>");
                        qCSBS.AppendFormat("     <condition attribute='new_loaigocmia' operator='eq' value='{0}' />", ((OptionSetValue)CTHDMia["new_loaigocmia"]).Value);
                        qCSBS.AppendFormat("     <condition attribute='new_loaigocmia' operator='null' />");
                        qCSBS.AppendFormat("</filter>");
                    }
                    else qCSBS.AppendFormat("<condition attribute='new_loaigocmia' operator='null' />");

                    if (KhachHang.Contains("new_nhomkhachhang"))
                    {
                        qCSBS.AppendFormat("<filter type='or'>");
                        qCSBS.AppendFormat("     <condition attribute='new_nhomkhachhang' operator='eq' value='{0}' />", ((EntityReference)KhachHang["new_nhomkhachhang"]).Id);
                        qCSBS.AppendFormat("     <condition attribute='new_nhomkhachhang' operator='null' />");
                        qCSBS.AppendFormat("</filter>");
                    }
                    else qCSBS.AppendFormat("<condition attribute='new_nhomkhachhang' operator='null' />");

                    if (CTHDMia.Contains("new_mohinhkhuyennong"))
                    {
                        qCSBS.AppendFormat("<filter type='or'>");
                        qCSBS.AppendFormat("     <condition attribute='new_mohinhkhuyennong' operator='eq' value='{0}' />", ((EntityReference)CTHDMia["new_thamgiamohinhkhuyennong"]).Id);
                        qCSBS.AppendFormat("     <condition attribute='new_mohinhkhuyennong' operator='null' />");
                        qCSBS.AppendFormat("</filter>");
                    }
                    else qCSBS.AppendFormat("<condition attribute='new_mohinhkhuyennong' operator='null' />");

                    if (CTHDMia.Contains("new_nhomculy"))
                    {
                        qCSBS.AppendFormat("<filter type='or'>");
                        qCSBS.AppendFormat("     <condition attribute='new_nhomculy' operator='eq' value='{0}' />", ((EntityReference)CTHDMia["new_nhomculy"]).Id);
                        qCSBS.AppendFormat("     <condition attribute='new_nhomculy' operator='null' />");
                        qCSBS.AppendFormat("</filter>");
                    }
                    else qCSBS.AppendFormat("<condition attribute='new_nhomculy' operator='null' />");

                    qCSBS.AppendFormat("</filter>");
                    qCSBS.AppendFormat("</entity>");
                    qCSBS.AppendFormat("</fetch>");

                    EntityCollection dsKKPTThuaDat = RetrieveNNRecord(service, "new_khuyenkhichphattrien", "new_thuadatcanhtac", "new_new_chitiethddtmia_new_khuyenkhichpt",
                        new ColumnSet(new string[] { "new_khuyenkhichphattrienid" }), "new_thuadatcanhtacid", CTHDMia.Id);
                    traceService.Trace("DSKKTPT: " + dsKKPTThuaDat.Entities.Count.ToString());

                    EntityCollection dsCSBS = service.RetrieveMultiple(new FetchExpression(qCSBS.ToString()));
                    traceService.Trace("so CSBS :" + dsCSBS.Entities.Count.ToString());
                    //traceService.Trace(dsCSBS[0]["new_machinhsach"].ToString());
                    if (dsCSBS != null && dsCSBS.Entities.Count > 0)
                    {
                        for (int i = 0; i < dsCSBS.Entities.Count; i++)
                        {
                            Entity cs = dsCSBS.Entities[i];

                            if (cs.Contains("new_nhomnangsuat"))
                            {
                                decimal nsbinhquan = (KhachHang.Contains("new_nangsuatbinhquan") ? (decimal)KhachHang["new_nangsuatbinhquan"] : 0);
                                Entity NhomNS = service.Retrieve("new_nhomnangsuat", ((EntityReference)cs["new_nhomnangsuat"]).Id, new ColumnSet(new string[] { "new_name", "new_nangsuattu", "new_nangsuatden" }));
                                if (!NhomNS.Contains("new_nangsuattu") || !NhomNS.Contains("new_nangsuatden")) throw new Exception("Thông tin chi tiết về nhóm năng suất " + NhomNS["new_name"].ToString() + " chưa có, vui lòng cập nhật !");
                                if (!(nsbinhquan >= (decimal)NhomNS["new_nangsuattu"] && nsbinhquan <= (decimal)NhomNS["new_nangsuatden"]))
                                {
                                    dsCSBS.Entities.RemoveAt(i);
                                    i--;
                                    continue;
                                }
                            }
                            traceService.Trace("pass nhom nang suat");
                            if (cs.Contains("new_khuyenkhichphattrien"))
                            {
                                bool check = false;
                                if (dsKKPTThuaDat.Entities.Count > 0)
                                    foreach (Entity t2 in dsKKPTThuaDat.Entities)
                                        if (((EntityReference)cs["new_khuyenkhichphattrien"]).Id == t2.Id)
                                        {
                                            check = true;
                                            break;
                                        }
                                if (!check)
                                {
                                    dsCSBS.Entities.RemoveAt(i);
                                    i--;
                                    continue;
                                }
                            }
                            traceService.Trace("pass KKPT");
                            EntityCollection csVungDL = RetrieveNNRecord(service, "new_vung", "new_chinhsachdautuchitiet", "new_new_chinhsachdautuchitiet_new_vung",
                                new ColumnSet(new string[] { "new_vungid" }), "new_chinhsachdautuchitietid", cs.Id);

                            if (csVungDL.Entities.Count > 0)
                            {
                                bool check = false;
                                List<Guid> dsvung = new List<Guid>();
                                foreach (Entity n in csVungDL.Entities)
                                    dsvung.Add(n.Id);

                                Entity diachi = service.Retrieve("new_diachi", ((EntityReference)ThuaDat["new_diachi"]).Id,
                                    new ColumnSet(new string[] { "new_path" }));

                                if (!diachi.Contains("new_path"))
                                    throw new Exception("Thông tin địa chỉ trên thửa đất thiếu thông tin, vui lòng cập nhật !");
                                QueryExpression qe = new QueryExpression("new_vungdialy_hanhchinh");
                                qe.NoLock = true;
                                qe.ColumnSet = new ColumnSet(new string[] { "new_vungdialy_hanhchinhid", "new_vungdialy", "new_path" });
                                qe.Criteria.AddCondition(new ConditionExpression("new_vungdialy", ConditionOperator.In, dsvung.ToArray()));

                                foreach (Entity n in service.RetrieveMultiple(qe).Entities)
                                    if (diachi["new_path"].ToString().Contains(n["new_path"].ToString()))
                                    {
                                        check = true;
                                        break;
                                    }
                                if (!check)
                                {
                                    dsCSBS.Entities.RemoveAt(i);
                                    i--;
                                    continue;
                                }
                            }

                            traceService.Trace("Pass dia chi");
                        }
                    }
                    decimal tBSHL = 0;
                    decimal tBSKHL = 0;
                    decimal tPB = 0;
                    EntityReferenceCollection fCSBS = new EntityReferenceCollection();
                    traceService.Trace("số CSBS lấy dc :" + dsCSBS.Entities.Count.ToString());
                    foreach (Entity a in dsCSBS.Entities)
                    {
                        traceService.Trace("Mã chinh sách: " + a["new_machinhsach"].ToString());
                        fCSBS.Add(a.ToEntityReference());
                        tBSHL += (a.Contains("new_sotienbosung") ? ((Money)a["new_sotienbosung"]).Value : 0);
                        tBSKHL += (a.Contains("new_sotienbosung_khl") ? ((Money)a["new_sotienbosung_khl"]).Value : 0);
                        tPB += (a.Contains("new_bosungphanbon") ? ((Money)a["new_bosungphanbon"]).Value : 0);
                    }
                    if (dsCSBS.Entities.Count > 0)
                        service.Associate("new_thuadatcanhtac", CTHDMia.Id, new Relationship("new_new_thuadatcanhtac_new_chinhsachdautuct"), fCSBS);

                    #endregion

                    /////////////// Lay CSDT bổ sung điền field Định mức bổ sung tối đa, không xét đk nghiệm thu
                    StringBuilder qCSBS_bstoida = new StringBuilder();
                    qCSBS_bstoida.AppendFormat("<fetch mapping='logical' version='1.0' no-lock='true'>");
                    qCSBS_bstoida.AppendFormat("<entity name='new_chinhsachdautuchitiet'>");
                    qCSBS_bstoida.AppendFormat("<attribute name='{0}' />", "new_sotienbosung_khl");
                    qCSBS_bstoida.AppendFormat("<attribute name='{0}' />", "new_sotienbosung");
                    qCSBS_bstoida.AppendFormat("<attribute name='{0}' />", "new_bosungphanbon");
                    qCSBS_bstoida.AppendFormat("<attribute name='{0}' />", "new_bosungtienmat");
                    qCSBS_bstoida.AppendFormat("<attribute name='{0}' />", "new_nhomnangsuat");
                    qCSBS_bstoida.AppendFormat("<attribute name='{0}' />", "new_khuyenkhichphattrien");
                    qCSBS_bstoida.AppendFormat("<filter type='and'>");
                    qCSBS_bstoida.AppendFormat("<condition attribute='new_vudautu' operator='eq' value='{0}' />", ((EntityReference)HDDTMia["new_vudautu"]).Id.ToString());
                    qCSBS_bstoida.AppendFormat("<condition attribute='statecode' operator='eq' value='{0}' />", 0);
                    qCSBS_bstoida.AppendFormat("<condition attribute='new_tungay' operator='le' value='{0}' />", ((DateTime)CTHDMia["createdon"]).AddHours(7).ToString("yyyy/MM/dd HH:mm:ss"));
                    qCSBS_bstoida.AppendFormat("<condition attribute='new_denngay' operator='ge' value='{0}' />", ((DateTime)CTHDMia["createdon"]).AddHours(7).ToString("yyyy/MM/dd HH:mm:ss"));

                    if (CTHDMia.Contains("new_loaisohuudat"))
                    {
                        qCSBS_bstoida.AppendFormat("<filter type='or'>");
                        qCSBS_bstoida.AppendFormat("     <condition attribute='new_loaisohuudat' operator='eq' value='{0}' />", ((OptionSetValue)CTHDMia["new_loaisohuudat"]).Value);
                        qCSBS_bstoida.AppendFormat("     <condition attribute='new_loaisohuudat' operator='null' />");
                        qCSBS_bstoida.AppendFormat("</filter>");
                    }
                    else qCSBS_bstoida.AppendFormat("<condition attribute='new_loaisohuudat' operator='null' />");

                    if (CTHDMia.Contains("new_giongmia"))
                    {
                        qCSBS_bstoida.AppendFormat("<filter type='or'>");
                        qCSBS_bstoida.AppendFormat("     <condition attribute='new_giongmia' operator='eq' value='{0}' />", ((EntityReference)CTHDMia["new_giongmia"]).Id);
                        qCSBS_bstoida.AppendFormat("     <condition attribute='new_giongmia' operator='null' />");
                        qCSBS_bstoida.AppendFormat("</filter>");
                    }
                    else qCSBS_bstoida.AppendFormat("<condition attribute='new_giongmia' operator='null' />");

                    if (KhachHang.Contains("new_nhomkhachhang"))
                    {
                        qCSBS_bstoida.AppendFormat("<filter type='or'>");
                        qCSBS_bstoida.AppendFormat("     <condition attribute='new_nhomkhachhang' operator='eq' value='{0}' />", ((EntityReference)KhachHang["new_nhomkhachhang"]).Id);
                        qCSBS_bstoida.AppendFormat("     <condition attribute='new_nhomkhachhang' operator='null' />");
                        qCSBS_bstoida.AppendFormat("</filter>");
                    }
                    else qCSBS_bstoida.AppendFormat("<condition attribute='new_nhomkhachhang' operator='null' />");

                    if (CTHDMia.Contains("new_mohinhkhuyennong"))
                    {
                        qCSBS_bstoida.AppendFormat("<filter type='or'>");
                        qCSBS_bstoida.AppendFormat("     <condition attribute='new_mohinhkhuyennong' operator='eq' value='{0}' />", ((EntityReference)CTHDMia["new_thamgiamohinhkhuyennong"]).Id);
                        qCSBS_bstoida.AppendFormat("     <condition attribute='new_mohinhkhuyennong' operator='null' />");
                        qCSBS_bstoida.AppendFormat("</filter>");
                    }
                    else qCSBS_bstoida.AppendFormat("<condition attribute='new_mohinhkhuyennong' operator='null' />");

                    if (CTHDMia.Contains("new_nhomculy"))
                    {
                        qCSBS_bstoida.AppendFormat("<filter type='or'>");
                        qCSBS_bstoida.AppendFormat("     <condition attribute='new_nhomculy' operator='eq' value='{0}' />", ((EntityReference)CTHDMia["new_nhomculy"]).Id);
                        qCSBS_bstoida.AppendFormat("     <condition attribute='new_nhomculy' operator='null' />");
                        qCSBS_bstoida.AppendFormat("</filter>");
                    }
                    else qCSBS_bstoida.AppendFormat("<condition attribute='new_nhomculy' operator='null' />");

                    qCSBS_bstoida.AppendFormat("</filter>");
                    qCSBS_bstoida.AppendFormat("</entity>");
                    qCSBS_bstoida.AppendFormat("</fetch>");

                    EntityCollection dsCSBS_bstoida = service.RetrieveMultiple(new FetchExpression(qCSBS_bstoida.ToString()));
                    if (dsCSBS_bstoida != null && dsCSBS_bstoida.Entities.Count > 0)
                    {
                        for (int i = 0; i < dsCSBS_bstoida.Entities.Count; i++)
                        {
                            Entity cs = dsCSBS_bstoida.Entities[i];

                            if (cs.Contains("new_nhomnangsuat"))
                            {
                                decimal nsbinhquan = (KhachHang.Contains("new_nangsuatbinhquan") ? (decimal)KhachHang["new_nangsuatbinhquan"] : 0);
                                Entity NhomNS = service.Retrieve("new_nhomnangsuat", ((EntityReference)cs["new_nhomnangsuat"]).Id, new ColumnSet(new string[] { "new_name", "new_nangsuattu", "new_nangsuatden" }));
                                if (!NhomNS.Contains("new_nangsuattu") || !NhomNS.Contains("new_nangsuatden")) throw new Exception("Thông tin chi tiết về nhóm năng suất " + NhomNS["new_name"].ToString() + " chưa có, vui lòng cập nhật !");
                                if (!(nsbinhquan >= (decimal)NhomNS["new_nangsuattu"] && nsbinhquan <= (decimal)NhomNS["new_nangsuatden"]))
                                {
                                    dsCSBS_bstoida.Entities.RemoveAt(i);
                                    i--;
                                    continue;
                                }
                            }

                            if (cs.Contains("new_khuyenkhichphattrien"))
                            {
                                bool check = false;
                                if (dsKKPTThuaDat.Entities.Count > 0)
                                    foreach (Entity t2 in dsKKPTThuaDat.Entities)
                                        if (((EntityReference)cs["new_khuyenkhichphattrien"]).Id == t2.Id)
                                        {
                                            check = true;
                                            break;
                                        }
                                if (!check)
                                {
                                    dsCSBS_bstoida.Entities.RemoveAt(i);
                                    i--;
                                    continue;
                                }
                            }

                            EntityCollection csVungDL = RetrieveNNRecord(service, "new_vung", "new_chinhsachdautuchitiet", "new_new_chinhsachdautuchitiet_new_vung", new ColumnSet(new string[] { "new_vungid" }), "new_chinhsachdautuchitietid", cs.Id);
                            if (csVungDL.Entities.Count > 0)
                            {
                                bool check = false;
                                List<Guid> dsvung = new List<Guid>();
                                foreach (Entity n in csVungDL.Entities)
                                    dsvung.Add(n.Id);

                                Entity diachi = service.Retrieve("new_diachi", ((EntityReference)ThuaDat["new_diachi"]).Id, new ColumnSet(new string[] { "new_path" }));
                                if (!diachi.Contains("new_path"))
                                    throw new Exception("Thông tin địa chỉ trên thửa đất thiếu thông tin, vui lòng cập nhật !");
                                QueryExpression qe = new QueryExpression("new_vungdialy_hanhchinh");
                                qe.NoLock = true;
                                qe.ColumnSet = new ColumnSet(new string[] { "new_vungdialy_hanhchinhid", "new_vungdialy", "new_path" });
                                qe.Criteria.AddCondition(new ConditionExpression("new_vungdialy", ConditionOperator.In, dsvung.ToArray()));

                                foreach (Entity n in service.RetrieveMultiple(qe).Entities)
                                    if (diachi["new_path"].ToString().Contains(n["new_path"].ToString()))
                                    {
                                        check = true;
                                        break;
                                    }
                                if (!check)
                                {
                                    dsCSBS_bstoida.Entities.RemoveAt(i);
                                    i--;
                                    continue;
                                }
                            }
                        }
                    }
                    decimal bsBSHL = 0;
                    decimal bsBSKHL = 0;
                    decimal bsPB = 0;

                    if (dsCSBS_bstoida != null && dsCSBS_bstoida.Entities.Count > 0)
                    {
                        foreach (Entity a in dsCSBS_bstoida.Entities)
                        {
                            bsBSHL += (a.Contains("new_sotienbosung") ? ((Money)a["new_sotienbosung"]).Value : 0);
                            bsBSKHL += (a.Contains("new_sotienbosung_khl") ? ((Money)a["new_sotienbosung_khl"]).Value : 0);
                            bsPB += (a.Contains("new_bosungphanbon") ? ((Money)a["new_bosungphanbon"]).Value : 0);
                        }
                    }

                    up["new_dinhmuctoida"] = new Money(bsBSHL + bsBSKHL + bsPB);
                    traceService.Trace("new_dinhmuctoida");

                    //////////// END --- Lay CSDT bổ sung điền field Định mức bổ sung tối đa

                    up["new_dongiadautukhonghoanlai"] = new Money(tBSKHL + (CSDautu.Contains("new_dinhmucdautukhonghoanlai") ? ((Money)CSDautu["new_dinhmucdautukhonghoanlai"]).Value : 0));
                    traceService.Trace("new_dongiadautukhonghoanlai");
                    up["new_dongiahopdong"] = new Money(tBSHL + (CSDautu.Contains("new_dinhmucdautuhoanlai") ? ((Money)CSDautu["new_dinhmucdautuhoanlai"]).Value : 0));
                    up["new_dongiadautuhoanlai"] = new Money(tBSHL + (CSDautu.Contains("new_dinhmucdautuhoanlai") ? ((Money)CSDautu["new_dinhmucdautuhoanlai"]).Value : 0));
                    up["new_dongiahopdongkhl"] = new Money(tBSKHL + (CSDautu.Contains("new_dinhmucdautukhonghoanlai") ? ((Money)CSDautu["new_dinhmucdautukhonghoanlai"]).Value : 0));
                    up["new_dinhmucdautukhonghoanlai"] = new Money((CTHDMia.Contains("new_dientichhopdong") ? (decimal)CTHDMia["new_dientichhopdong"] : (decimal)0) * ((Money)up["new_dongiadautukhonghoanlai"]).Value);
                    traceService.Trace("new_dinhmucdautukhonghoanlai");

                    traceService.Trace("new_dongiadautuhoanlai");
                    up["new_dinhmucdautuhoanlai"] = new Money((CTHDMia.Contains("new_dientichhopdong") ? (decimal)CTHDMia["new_dientichhopdong"] : (decimal)0) * ((Money)up["new_dongiadautuhoanlai"]).Value);
                    traceService.Trace("new_dinhmucdautuhoanlai");

                    if (!(HDDTMia.Contains("new_chinhantienmat") && (bool)HDDTMia["new_chinhantienmat"]))
                    {
                        up["new_dongiaphanbontoithieu"] = new Money(tPB + (CSDautu.Contains("new_dinhmucphanbontoithieu") ? ((Money)CSDautu["new_dinhmucphanbontoithieu"]).Value : 0));
                        up["new_dongiaphanbonhd"] = new Money(tPB + (CSDautu.Contains("new_dinhmucphanbontoithieu") ? ((Money)CSDautu["new_dinhmucphanbontoithieu"]).Value : 0));
                        traceService.Trace("new_dongiaphanbontoithieu");
                        up["new_dinhmucphanbontoithieu"] = new Money((CTHDMia.Contains("new_dientichhopdong") ? (decimal)CTHDMia["new_dientichhopdong"] : new decimal(0)) * ((Money)up["new_dongiaphanbontoithieu"]).Value);
                        traceService.Trace("new_dinhmucphanbontoithieu");
                    }

                    up["new_dinhmucdautu"] = new Money(((Money)up["new_dinhmucdautukhonghoanlai"]).Value + ((Money)up["new_dinhmucdautuhoanlai"]).Value);
                    
                    traceService.Trace("new_dinhmucdautu");

                    // Gen ty le thu hoi von du kien
                    #region Gen ty le thu hoi von du kien

                    // Xoa ty le thu hoi von du kien cu
                    EntityCollection oldlTLTHVDK = FindTLTHVDK(service, CTHDMia);
                    if (oldlTLTHVDK != null || oldlTLTHVDK.Entities.Count > 0)
                    {
                        foreach (Entity a in oldlTLTHVDK.Entities)
                        {
                            service.Delete("new_tylethuhoivondukien", a.Id);
                        }
                    }

                    traceService.Trace("Xoa xong ty le thu hoi von dk");

                    // End Xoa ty le thu hoi von du kien cu

                    EntityCollection collTLTHV = FindTLTHVinCSDT(service, CSDautu);
                    Entity csdtKQEntity = service.Retrieve("new_chinhsachdautu", CSDautu.Id, new ColumnSet(new string[] { "new_dinhmucdautuhoanlai" }));

                    //traceService.Trace("so record TLTHVDK " + collTLTHV.Entities.Count());
                    //throw new Exception("chay plugin tim CSDT 1");

                    if (collTLTHV != null && collTLTHV.Entities.Count > 0)
                    {
                        traceService.Trace("so record TLTHVDK " + collTLTHV.Entities.Count());

                        foreach (Entity TLTHV in collTLTHV.Entities)
                        {
                            Entity tlthvdkHDCT = new Entity("new_tylethuhoivondukien");

                            //EntityReference vudautuEntityRef = HDDTmia.GetAttributeValue<EntityReference>("new_vudautu");
                            EntityReference hdctEntityRef = new EntityReference("new_thuadatcanhtac", CTHDMia.Id);
                            traceService.Trace("chi tiet HDDT ref");

                            if (TLTHV.Contains("new_phantramtilethuhoi") && TLTHV.Contains("new_vuthuhoi") && csdtKQEntity.Contains("new_dinhmucdautuhoanlai"))
                            {
                                traceService.Trace("gen thu hoi du kien");

                                EntityReference vdtRef = TLTHV.GetAttributeValue<EntityReference>("new_vuthuhoi");
                                Entity vdt = service.Retrieve("new_vudautu", vdtRef.Id, new ColumnSet(new string[] { "new_name" }));

                                traceService.Trace("gen thu hoi du kien 1");

                                string tenvdt = vdt.Contains("new_name") ? vdt["new_name"].ToString() : "";
                                string tenTLTHVDK = "Tỷ lệ thu hồi " + tenvdt;
                                decimal tyle = (TLTHV.Contains("new_phantramtilethuhoi") ? (decimal)TLTHV["new_phantramtilethuhoi"] : 0);
                                decimal sotienDTHL = (up.Contains("new_dautuhoanlai") ? up.GetAttributeValue<Money>("new_dautuhoanlai").Value : (tBSHL + (CSDautu.Contains("new_dinhmucdautuhoanlai") ? ((Money)CSDautu["new_dinhmucdautuhoanlai"]).Value : 0)) * (CTHDMia.Contains("new_dientichhopdong") ? (decimal)CTHDMia["new_dientichhopdong"] : (decimal)0));
                                decimal sotien = (sotienDTHL * tyle) / 100;

                                traceService.Trace("gen thu hoi du kien 2");

                                Money sotienM = new Money(sotien);

                                tlthvdkHDCT.Attributes.Add("new_name", tenTLTHVDK);
                                tlthvdkHDCT.Attributes.Add("new_loaityle", new OptionSetValue(100000000));
                                tlthvdkHDCT.Attributes.Add("new_chitiethddtmia", hdctEntityRef);
                                tlthvdkHDCT.Attributes.Add("new_vudautu", vdtRef);
                                tlthvdkHDCT.Attributes.Add("new_tylephantram", tyle);
                                tlthvdkHDCT.Attributes.Add("new_sotienthuhoi", sotienM);

                                service.Create(tlthvdkHDCT);
                                traceService.Trace("tao xong thu hoi du kien");
                            }
                            traceService.Trace("tao xong thu hoi du kien  aa");
                        }
                        traceService.Trace("tao xong thu hoi du kien  bb");
                    }
                    //throw new Exception("Gan xong TLTHVDK");

                    #endregion

                    //End Gen ty le thu hoi von du kien
                    traceService.Trace("truoc update chi tiet HD");
                    service.Update(up);
                    traceService.Trace("sau update chi tiet HD");
                }
            }
        }

        public bool CheckRunUpdate(Entity target)
        {
            if (target.Contains("new_phuluchopdongid"))
                return false;

            string[] list = new string[] { "new_vutrong", "new_mucdichsanxuatmia","new_loaisohuudat","new_loaigocmia",
                "new_luugoc","new_giongmia","new_thuadat","new_thamgiamohinhkhuyennong"};
            foreach (string a in list)
            {
                if (target.Contains(a))
                    return true;
            }
            return false;
        }

        public static EntityCollection GetCSDTFromHDMia(IOrganizationService crmservices, string entity1, string entity2, string relateName, ColumnSet column, string condition, object value, string order, Guid vudautu)
        {
            QueryExpression query = new QueryExpression(entity1);
            query.ColumnSet = column;
            query.NoLock = true;
            query.Orders.Add(new OrderExpression(order, OrderType.Descending));
            LinkEntity linkEntity1 = new LinkEntity(entity1, relateName, entity1 + "id", entity1 + "id", JoinOperator.Inner);
            LinkEntity linkEntity2 = new LinkEntity(relateName, entity2, entity2 + "id", entity2 + "id", JoinOperator.Inner);

            linkEntity1.LinkEntities.Add(linkEntity2);
            query.LinkEntities.Add(linkEntity1);

            linkEntity2.LinkCriteria = new FilterExpression();
            linkEntity2.LinkCriteria.AddCondition(new ConditionExpression(condition, ConditionOperator.Equal, value));
            query.Criteria.AddCondition(new ConditionExpression("statecode", ConditionOperator.Equal, 0));
            query.Criteria.AddCondition(new ConditionExpression("new_vudautu", ConditionOperator.Equal, vudautu));
            EntityCollection collRecords = crmservices.RetrieveMultiple(query);

            return collRecords;
        }

        public static EntityCollection RetrieveNNRecord(IOrganizationService crmservices, string entity1, string entity2, string relateName, ColumnSet column, string condition, object value)
        {
            QueryExpression query = new QueryExpression(entity1);
            query.ColumnSet = column;
            query.NoLock = true;
            LinkEntity linkEntity1 = new LinkEntity(entity1, relateName, entity1 + "id", entity1 + "id", JoinOperator.Inner);
            LinkEntity linkEntity2 = new LinkEntity(relateName, entity2, entity2 + "id", entity2 + "id", JoinOperator.Inner);

            linkEntity1.LinkEntities.Add(linkEntity2);
            query.LinkEntities.Add(linkEntity1);

            linkEntity2.LinkCriteria = new FilterExpression();
            linkEntity2.LinkCriteria.AddCondition(new ConditionExpression(condition, ConditionOperator.Equal, value));
            EntityCollection collRecords = crmservices.RetrieveMultiple(query);

            return collRecords;
        }

        public static EntityCollection FindTLTHVinCSDT(IOrganizationService crmservices, Entity CSDT)
        {
            string fetchXml =
                   @"<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='false'>
                                      <entity name='new_tilethuhoivon'>
                                        <attribute name='new_name' />
                                        <attribute name='new_phantramtilethuhoi' />
                                        <attribute name='new_vuthuhoi' />
                                        <attribute name='new_chinhsachdautu' />
                                        <attribute name='new_chinhsachdautubosung' />
                                        <attribute name='new_tilethuhoivonid' />
                                        <order attribute='new_nam' descending='false' />
                                        <link-entity name='new_chinhsachdautu' from='new_chinhsachdautuid' to='new_chinhsachdautu' alias='ac'>
                                          <filter type='and'>
                                            <condition attribute='statecode' operator='eq' value='0' />
                                            <condition attribute='new_chinhsachdautuid' operator='eq' uitype='new_chinhsachdautu' value='{0}' />
                                          </filter>
                                        </link-entity>
                                      </entity>
                                    </fetch>";

            fetchXml = string.Format(fetchXml, CSDT.Id);
            EntityCollection entc = crmservices.RetrieveMultiple(new FetchExpression(fetchXml));

            return entc;
        }

        public static EntityCollection FindTLTHVDK(IOrganizationService crmservices, Entity chitietHD)
        {
            string fetchXml =
                   @"<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='false'>
                      <entity name='new_tylethuhoivondukien'>
                        <attribute name='new_name' />
                        <attribute name='new_vudautu' />
                        <attribute name='new_tylephantram' />
                        <attribute name='new_sotienthuhoi' />
                        <attribute name='new_loaityle' />
                        <attribute name='new_chitiethddtmia' />
                        <attribute name='new_tylethuhoivondukienid' />
                        <order attribute='new_name' descending='false' />
                        <filter type='and'>
                          <condition attribute='new_chitiethddtmia' operator='eq' uitype='new_thuadatcanhtac' value='{0}' />
                        </filter>
                      </entity>
                    </fetch>";

            fetchXml = string.Format(fetchXml, chitietHD.Id);
            EntityCollection entc = crmservices.RetrieveMultiple(new FetchExpression(fetchXml));

            return entc;
        }


    }
}
