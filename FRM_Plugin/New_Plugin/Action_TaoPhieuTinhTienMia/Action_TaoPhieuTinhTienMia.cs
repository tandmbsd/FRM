using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk.Messages;
using System.Text;
using System.Linq;
using System.IO;
using System.Xml;

namespace Action_TaoPhieuTinhTienMia
{
    public class Action_TaoPhieuTinhTienMia : IPlugin
    {
        public EntityCollection NhomNS;
        public EntityReference target;
        public Entity VuDauTu;
        public Dictionary<Guid, decimal> TamgiutheoThua = new Dictionary<Guid, decimal>(); //{CTHDMia ; totaltamgiu}
        public Dictionary<Guid, decimal> TamgiutheoHDTH = new Dictionary<Guid, decimal>(); //{HDTH ; totaltamgiu}
        public Dictionary<Guid, decimal> TamgiutheoHDVC = new Dictionary<Guid, decimal>(); //{HDVC ; totaltamgiu}
        public Dictionary<Guid, int> DSGiong = new Dictionary<Guid, int>();
        public Dictionary<Guid, Entity> DSBBThuHoachSom = new Dictionary<Guid, Entity>();
        public EntityCollection DSTTCongDon;
        public EntityCollection BangGiaVanChuyen;

        public Dictionary<Guid, Entity> DSPBDT_KHCN = new Dictionary<Guid, Entity>(); //{contactID, PBDT}
        public Dictionary<Guid, Entity> DSPBDT_KHDN = new Dictionary<Guid, Entity>(); //{accountID, PBDT}

        IOrganizationService service = null;
        IOrganizationServiceFactory factory = null;
        void IPlugin.Execute(IServiceProvider serviceProvider)
        {
            IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            EntityReference target = (EntityReference)context.InputParameters["Target"];
            factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
            service = factory.CreateOrganizationService(context.UserId);

            try
            {
                //clear phiếu tính tiền mía cũ
                QueryExpression qe = new QueryExpression("new_phieutinhtienmia");
                qe.ColumnSet = new ColumnSet("new_phieutinhtienmiaid");
                qe.Criteria.Conditions.Add(new ConditionExpression("new_bangke", ConditionOperator.Equal, target.Id));
                foreach (Entity a in service.RetrieveMultiple(qe).Entities)
                    service.Delete("new_phieutinhtienmia", a.Id);
                //clear PDN thu nợ cũ
                ///.....

                //Start get chính sách

                Entity LenhChi = service.Retrieve("new_bangketienmia", target.Id, new ColumnSet(true));
                Entity Vuthuhoach = service.Retrieve("new_vuthuhoach", ((EntityReference)LenhChi["new_vuthuhoach"]).Id, new ColumnSet(true));
                VuDauTu = service.Retrieve("new_vudautu", ((EntityReference)Vuthuhoach["new_vudautu"]).Id, new ColumnSet("new_loaitrichthu", "new_giatri"));

                calculateSumTamGiu(VuDauTu.Id);
                calculateSumTamGiuThuHoach(VuDauTu.Id);
                calculateSumTamGiuVanChuyen(VuDauTu.Id);
                loadGiong();
                loadBBThuHoachSom(VuDauTu.Id);

                NhomNS = GetNhomNangSuat(VuDauTu.Id);

                EntityCollection CSTM = GetChinhSachThuMua(VuDauTu.Id);
                EntityCollection DSLenhdon = GetAllLenhDon(target.Id);
                loadBBThoaThuanCongDon(VuDauTu.Id);
                loadBangGiaVanChuyen(VuDauTu.Id);

                //Check CS in HD dau tu mia
                foreach (Entity a in GetAllHDMiainCS(VuDauTu.Id).Entities)
                {
                    foreach (Entity ld in DSLenhdon.Entities.Where(o => o["new_hopdongdautumia"] == new EntityReference("new_hopdongdautumia", (Guid)((AliasedValue)a["b.new_hopdongdautumiaid"]).Value)))
                    {
                        if ((DateTime)ld["new_thoigiancanra"] >= (DateTime)a["new_thoidiemapdung"])
                            if (!ld.Contains("ngayapdung"))
                            {
                                ld["csid"] = a["new_chinhsachthumuaid"];
                                ld["ngayapdung"] = a["new_thoidiemapdung"];
                                CreatePTTM(ld, a);
                            }
                    }
                }

                //Check CS in HD mia ngoai
                foreach (Entity a in GetAllHDMuaNgoaiinCS(VuDauTu.Id).Entities)
                {
                    foreach (Entity ld in DSLenhdon.Entities.Where(o => o["new_hopdongmuabanmiangoai"] == new EntityReference("new_hopdongmuabanmiangoai", (Guid)((AliasedValue)a["b.new_hopdongmuabanmiangoaiid"]).Value)))
                    {
                        if ((DateTime)ld["new_thoigiancanra"] >= (DateTime)a["new_thoidiemapdung"])
                            if (!ld.Contains("ngayapdung"))
                            {
                                ld["csid"] = a["new_chinhsachthumuaid"];
                                ld["ngayapdung"] = a["new_thoidiemapdung"];
                                CreatePTTM(ld, a);
                            }
                    }
                }

                //Check all Lenh don HD mia con lai
                foreach (Entity ld in DSLenhdon.Entities.Where(o => (!o.Contains("ngayapdung") && o.Contains("new_hopdongdautumia"))))
                {
                    foreach (Entity cs in CSTM.Entities.Where(o => ((OptionSetValue)o["new_hoatdongapdung"]).Value == 100000000).OrderByDescending(o => o["new_thoidiemapdung"]))
                    {
                        if ((DateTime)ld["new_thoigiancanra"] >= (DateTime)cs["new_thoidiemapdung"])
                        {
                            if (CheckChinhSachHDMia(ld, cs))
                            {
                                ld["csid"] = cs["new_chinhsachthumuaid"];
                                ld["ngayapdung"] = cs["new_thoidiemapdung"];
                                CreatePTTM(ld, cs);
                                break;
                            }
                        }
                    }
                }

                //Check all Lenh don HD mia ngoai con lai
                foreach (Entity ld in DSLenhdon.Entities.Where(o => (!o.Contains("ngayapdung") && o.Contains("new_hopdongmuabanmiangoai"))))
                {
                    foreach (Entity cs in CSTM.Entities.Where(o => ((OptionSetValue)o["new_hoatdongapdung"]).Value == 100000003).OrderByDescending(o => o["new_thoidiemapdung"]))
                    {
                        if ((DateTime)ld["new_thoigiancanra"] >= (DateTime)cs["new_thoidiemapdung"])
                        {
                            if (CheckChinhSachHDMuaNgoai(ld, cs))
                            {
                                ld["csid"] = cs["new_chinhsachthumuaid"];
                                ld["ngayapdung"] = cs["new_thoidiemapdung"];
                                CreatePTTM(ld, cs);
                                break;
                            }
                        }
                    }
                }

                context.OutputParameters["ReturnId"] = "success";
            }
            catch (Exception ex)
            {
                context.OutputParameters["ReturnId"] = "error: " + ex.Message;
            }
        }

        EntityCollection RetrieveNNRecord(string entity1, string entity2, string relateName, ColumnSet column, string entity2condition, object entity2value)
        {
            EntityReferenceCollection result = new EntityReferenceCollection();
            QueryExpression query = new QueryExpression(entity1);
            query.ColumnSet = column;
            LinkEntity linkEntity1 = new LinkEntity(entity1, relateName, entity1 + "id", entity1 + "id", JoinOperator.Inner);
            LinkEntity linkEntity2 = new LinkEntity(relateName, entity2, entity2 + "id", entity2 + "id", JoinOperator.Inner);

            linkEntity1.LinkEntities.Add(linkEntity2);
            query.LinkEntities.Add(linkEntity1);

            linkEntity2.LinkCriteria = new FilterExpression();
            linkEntity2.LinkCriteria.AddCondition(new ConditionExpression(entity2condition, ConditionOperator.Equal, entity2value));
            EntityCollection collRecords = service.RetrieveMultiple(query);

            return collRecords;
        }

        EntityCollection GetAllLenhDon(Guid BangkeId)
        {
            EntityCollection result = new EntityCollection();

            //<link-entity name = 'new_giongmia' from='new_giongmiaid' to='new_giongtrongthucte' link-type='inner' alias='c'>
            //  <attribute name='new_nhomgiong' />
            //</link-entity>

            //<link-entity name = 'new_chitietbbthuhoachsom' from='new_chitiethddtmia' to='new_thuadatcanhtacid' link-type='outer' alias='h'>
            //  <attribute name='new_chitietbbthuhoachsomid' />
            //  <attribute name='new_baohiemccs' />
            //  <attribute name='new_tamgiu' />
            //  <attribute name='new_dientichthsom' />
            //  <attribute name='new_ngayhethan' />
            //</link-entity>

            string query = string.Format(@"<fetch version = '1.0' output-format='xml-platform' mapping='logical' distinct='true'>
                  <entity name = 'new_lenhdon' >
                    <attribute name='new_name' />
                    <attribute name='new_vudautu' />
                    <attribute name='new_hopdongvanchuyen' />
                    <attribute name='new_hopdongthuhoach' />
                    <attribute name='new_doitacthuhoach' />
                    <attribute name='new_doitacthuhoachkhdn' />
                    <attribute name='new_doitacvanchuyen' />
                    <attribute name='new_doitacvanchuyenkhdn' />
                    <attribute name='new_ngaycap' />
                    <attribute name='new_thuhoachmay' />
                    <attribute name='new_lenhdonid' />
                    <attribute name='new_loaihopdong' />
                    <attribute name='new_hopdongdautumia' />
                    <attribute name='new_thuacanhtac' />
                    <attribute name='new_hopdongmuabanmiangoai' />
                    <attribute name='new_khachhang' />
                    <attribute name='new_khachhangdoanhnghiep' />
                    <attribute name='new_miachay' />
                    <attribute name='new_miachaycoy' />
                    <attribute name='new_loaimiachay' />
                    <attribute name='new_miadonga' />
                    <attribute name='new_thoigiancanra' />
                    <attribute name='new_miadutuoi' />
                    <attribute name='new_trongluongxoi' />
                    <attribute name='new_trongluongbi' />
                    <attribute name='new_tapchatthucte' />
                    <attribute name='new_ccsthucte' />
                    <link-entity name = 'new_new_bangketienmia_new_lenhdon' from='new_lenhdonid' to='new_lenhdonid' link-type='inner' >
                      <link-entity name = 'new_bangketienmia' from='new_bangketienmiaid' to='new_bangketienmiaid' link-type='inner' >
                        <filter type = 'and' >
                          <condition attribute='new_bangketienmiaid' operator='eq' value='{0}' />
                        </filter>
                      </link-entity>
                    </link-entity>
                    <link-entity name = 'new_thuadatcanhtac' from='new_thuadatcanhtacid' to='new_thuacanhtac' link-type='outer' alias='b' >
                      <attribute name='new_vutrong' />
                      <attribute name='new_nhomdat' />
                      <attribute name='new_loaigocmia' />
                      <attribute name='new_luugoc' />
                      <attribute name='new_giongtrongthucte' />
                      <attribute name='new_loaisohuudat' />
                      <attribute name='new_mucdichsanxuatmia' />
                      <attribute name='new_culy' />
                      <attribute name='new_nhomculy' />
                      <attribute name='new_thamgiamohinhkhuyennong' />
                      <attribute name='new_donvitinh' />
                      <attribute name='new_cachtinhtamgiu' />
                      <attribute name='new_giatritamgiu' />
                      <attribute name='new_tamgiutoida' />
                      <attribute name='new_dientichconlai' />
                      <link-entity name = 'new_thuadat' from='new_thuadatid' to='new_thuadat' link-type='inner' alias='i'>
                        <attribute name='new_vungdialy' />
                        <link-entity name = 'new_diachi' from='new_diachiid' to='new_diachi' link-type='inner' alias='d'>
                          <attribute name='new_path' />
                        </link-entity>
                      </link-entity>
                      <link-entity name = 'account' from='accountid' to='new_khachhangdoanhnghiep' link-type='outer' alias='e' >
                        <attribute name='new_nhomkhachhang' />
                        <attribute name='new_nangsuatbinhquan' />
                      </link-entity>
                      <link-entity name = 'contact' from='contactid' to='new_khachhang' link-type='outer' alias='f' >
                        <attribute name='new_nhomkhachhang' />
                        <attribute name='new_nangsuatbinhquan' />
                      </link-entity>
                    </link-entity>
                    <link-entity name = 'new_hopdongmuabanmiangoai' from='new_hopdongmuabanmiangoaiid' to='new_hopdongmuabanmiangoai' link-type='outer' alias='g' >
                      <attribute name='new_culy' />
                      <attribute name='new_nhomculy' />
                      <attribute name='new_vungdialy' />
                    </link-entity>
                    <link-entity name = 'new_hopdongthuhoach' from='new_hopdongthuhoachid' to='new_hopdongthuhoach' link-type='outer' alias='hdth'>
                      <attribute name='new_dinhmuctientamgiu' />
                      <attribute name='new_tamgiutoida' />
                      <attribute name='new_tylethuhoi' />
                    </link-entity>
                    <link-entity name = 'new_hopdongvanchuyen' from='new_hopdongvanchuyenid' to='new_hopdongvanchuyen' link-type='outer' alias='hdvc'>
                      <attribute name='new_donvitinh' />
                      <attribute name='new_cachtinhtamgiu' />
                      <attribute name='new_giatritamgiu' />
                      <attribute name='new_tamgiutoida' />
                      <attribute name='new_tylethuhoi' />
                    </link-entity>
                    <filter type = 'and' >
                      <condition attribute='statuscode' operator='eq' value='100000002' />
                    </filter>
                  </entity>
                </fetch>", BangkeId.ToString());

            int fetchCount = 5000;
            int pageNumber = 1;
            string pagingCookie = null;

            while (true)
            {
                string xml = CreateXml(query, pagingCookie, pageNumber, fetchCount);
                RetrieveMultipleRequest fetchRequest1 = new RetrieveMultipleRequest
                {
                    Query = new FetchExpression(xml)
                };

                EntityCollection returnCollection = ((RetrieveMultipleResponse)service.Execute(fetchRequest1)).EntityCollection;
                if (returnCollection.Entities.Count > 0)
                    result.Entities.AddRange(returnCollection.Entities);

                if (returnCollection.MoreRecords)
                {
                    pageNumber++;
                    pagingCookie = returnCollection.PagingCookie;
                }
                else
                {
                    break;
                }
            }
            return result;
        }

        EntityCollection GetChinhSachThuMua(Guid VuDauTu)
        {
            QueryExpression qe = new QueryExpression("new_chinhsachthumua");
            qe.ColumnSet = new ColumnSet(true);
            qe.Orders.Add(new OrderExpression("new_thoidiemapdung", OrderType.Descending));

            FilterExpression f1 = new FilterExpression(LogicalOperator.And);
            f1.Conditions.Add(new ConditionExpression("new_vudautu", ConditionOperator.Equal, VuDauTu));
            f1.Conditions.Add(new ConditionExpression("statecode", ConditionOperator.Equal, 0));

            FilterExpression f2 = new FilterExpression(LogicalOperator.Or);
            f2.Conditions.Add(new ConditionExpression("new_hoatdongapdung", ConditionOperator.Equal, 100000000));
            f2.Conditions.Add(new ConditionExpression("new_hoatdongapdung", ConditionOperator.Equal, 100000003));
            f1.AddFilter(f2);

            qe.Criteria = f1;

            EntityCollection result = service.RetrieveMultiple(qe);

            foreach (Entity a in result.Entities)
            {
                //get vung dia ly
                string qvung = " <fetch version = '1.0' output-format='xml-platform' mapping='logical' distinct='true'> " +
                       " <entity name = 'new_vung' > " +
                         " <attribute name='new_vungid' /> " +
                         " <link-entity name = 'new_new_chinhsachthumua_new_vung' from='new_vungid' to='new_vungid' link-type='inner' > " +
                           " <link-entity name = 'new_chinhsachthumua' from='new_chinhsachthumuaid' to='new_chinhsachthumuaid' link-type='inner' > " +
                             " <filter type = 'and' > " +
                               " <condition attribute='new_chinhsachthumuaid' operator='eq' value='" + a.Id.ToString() + "' /> " +
                             " </filter> " +
                           " </link-entity> " +
                         " </link-entity> " +
                         " <link-entity name = 'new_vungdialy_hanhchinh' from='new_vungdialy' to='new_vungid' link-type='inner' alias='b' > " +
                           " <attribute name='new_path' /> " +
                         " </link-entity> " +
                       " </entity> " +
                     " </fetch> ";

                EntityCollection rvung = service.RetrieveMultiple(new FetchExpression(qvung));
                if (rvung.Entities.Count > 0)
                {
                    a["dspath"] = rvung.Entities.Select(o => ((AliasedValue)o["b.new_path"]).Value.ToString());
                    a["dsvung"] = rvung.Entities.GroupBy(o => (Guid)o["new_vungid"]).Select(k => k.First().Id);
                }

                //get Giống mía
                EntityCollection rGiong = RetrieveNNRecord("new_giongmia", "new_chinhsachthumua", "new_new_chinhsachthumua_new_giongmia", new ColumnSet("new_giongmiaid"), "new_chinhsachthumuaid", a.Id);
                if (rGiong.Entities.Count > 0)
                    a["dsgiong"] = rGiong.Entities.Select(o => (Guid)o["new_giongmiaid"]);

                //get Nhóm khách hàng
                EntityCollection rNhomKH = RetrieveNNRecord("new_nhomkhachhang", "new_chinhsachthumua", "new_new_chinhsachthumua_new_nhomkhachhang", new ColumnSet("new_nhomkhachhangid"), "new_chinhsachthumuaid", a.Id);
                if (rNhomKH.Entities.Count > 0)
                    a["dsnhomkh"] = rNhomKH.Entities.Select(o => (Guid)o["new_nhomkhachhangid"]);

                //get Nhóm năng suất
                EntityCollection rNangsuat = RetrieveNNRecord("new_nhomnangsuat", "new_chinhsachthumua", "new_new_chinhsachthumua_new_nhomnangsuat", new ColumnSet("new_nhomnangsuatid"), "new_chinhsachthumuaid", a.Id);
                if (rNangsuat.Entities.Count > 0)
                    a["dsnhomns"] = rNangsuat.Entities.Select(o => (Guid)o["new_nhomnangsuatid"]);

                //get Nhóm cự ly
                EntityCollection rNhomCuLy = RetrieveNNRecord("new_nhomculy", "new_chinhsachthumua", "new_new_chinhsachthumua_new_nhomculy", new ColumnSet("new_nhomculyid"), "new_chinhsachthumuaid", a.Id);
                if (rNhomCuLy.Entities.Count > 0)
                    a["dsnhomculy"] = rNhomCuLy.Entities.Select(o => (Guid)o["new_nhomculyid"]);

                //get Mô hình KN
                EntityCollection rKhuyenNong = RetrieveNNRecord("new_mohinhkhuyennong", "new_chinhsachthumua", "new_new_chinhsachthumua_new_mohinhkhuyennon", new ColumnSet("new_mohinhkhuyennongid"), "new_chinhsachthumuaid", a.Id);
                if (rKhuyenNong.Entities.Count > 0)
                    a["dsmhkn"] = rKhuyenNong.Entities.Select(o => (Guid)o["new_mohinhkhuyennongid"]);

                //get CCS Bao
                EntityCollection ccsbao = RetrieveMulti("new_chinhsachthumua_ccsbao", "new_chinhsachthumua", a.Id, "new_tu");
                if (ccsbao.Entities.Count > 0)
                    a["ccsbao"] = ccsbao;

                //get CCS Bao thu hoach som
                EntityCollection ccsbao_ths = RetrieveMulti("new_chinhsachthumua_ccsbao", "new_chinhsachthumua_thuhoachsom", a.Id, "new_tu");
                if (ccsbao_ths.Entities.Count > 0)
                    a["ccsbao_ths"] = ccsbao_ths;

                //get CCS Thưởng
                EntityCollection ccsthuong = RetrieveMulti("new_chinhsachthumua_ccsthuong", "new_chinhsachthumua", a.Id, "new_tu");
                if (ccsthuong.Entities.Count > 0)
                    a["ccsthuong"] = ccsthuong;

                //get Tạp chất thưởng
                EntityCollection tapchatthuong = RetrieveMulti("new_chinhsachthumuatapchatthuong", "new_chinhsachthumua", a.Id, "new_tu");
                if (tapchatthuong.Entities.Count > 0)
                    a["tapchatthuong"] = tapchatthuong;

                //get Tạp chất trừ
                EntityCollection tapchattru = RetrieveMulti("new_chinhsachthumuatapchattru", "new_chinhsachthumua", a.Id, "new_tu");
                if (tapchattru.Entities.Count > 0)
                    a["tapchattru"] = tapchattru;
            }

            return result;
        }

        EntityCollection RetrieveMulti(string entity, string cond, object value, string order)
        {
            QueryExpression qe = new QueryExpression(entity);
            qe.ColumnSet = new ColumnSet(true);
            qe.Criteria.Conditions.Add(new ConditionExpression(cond, ConditionOperator.Equal, value));
            qe.Criteria.Conditions.Add(new ConditionExpression("statecode", ConditionOperator.Equal, 0));
            qe.Orders.Add(new OrderExpression(order, OrderType.Ascending));

            return service.RetrieveMultiple(qe);
        }

        EntityCollection GetAllHDMiainCS(Guid VuDauTu)
        {
            string query = " <fetch version = '1.0' output-format='xml-platform' mapping='logical' distinct='true'> " +
                           " <entity name = 'new_chinhsachthumua' > " +
                            " <attribute name='new_chinhsachthumuaid' /> " +
                            " <attribute name='new_thoidiemapdung' /> " +
                            " <order attribute='new_thoidiemapdung' descending='true' />" +
                            " <link-entity name = 'new_new_chinhsachthumua_new_hopdongdautumia' from='new_chinhsachthumuaid' to='new_chinhsachthumuaid' link-type='inner' > " +
                              " <link-entity name = 'new_hopdongdautumia' from='new_hopdongdautumiaid' to='new_hopdongdautumiaid' link-type='inner' alias='b'> " +
                                   " <attribute name = 'new_hopdongdautumiaid' /> " +
                                   " </link-entity> " +
                            " </link-entity> " +
                            " <filter type = 'and' > " +
                              " <condition entityname='new_chinhsachthumua' attribute='new_vudautu' operator='eq' value='" + VuDauTu.ToString() + "' /> " +
                              " <condition entityname = 'new_chinhsachthumua' attribute='new_hoatdongapdung' operator='eq' value='100000000' /> " +
                              " <condition entityname = 'new_chinhsachthumua' attribute='statecode' operator='eq' value='0' /> " +
                            " </filter> " +
                          " </entity> " +
                        " </fetch> ";
            FetchExpression qe = new FetchExpression(query);
            return service.RetrieveMultiple(qe);
        }

        EntityCollection GetAllHDMuaNgoaiinCS(Guid VuDauTu)
        {
            string query = " <fetch version = '1.0' output-format='xml-platform' mapping='logical' distinct='true'> " +
                            " <entity name = 'new_chinhsachthumua' > " +
                            " <attribute name='new_chinhsachthumuaid' /> " +
                            " <attribute name='new_thoidiemapdung' /> " +
                            " <order attribute='new_thoidiemapdung' descending='true' />" +
                            " <link-entity name = 'new_new_chinhsachthumua_new_hopdongmuabanmi' from='new_chinhsachthumuaid' to='new_chinhsachthumuaid' link-type='inner' > " +
                                " <link-entity name = 'new_hopdongmuabanmiangoai' from='new_hopdongmuabanmiangoaiid' to='new_hopdongmuabanmiangoaiid' link-type='inner' > " +
                                " <attribute name = 'new_hopdongmuabanmiangoaiid' /> " +
                                " </link-entity> " +
                            " </link-entity> " +
                            " <filter type = 'and' > " +
                                " <condition entityname='new_chinhsachthumua' attribute='new_vudautu' operator='eq' value='" + VuDauTu.ToString() + "' /> " +
                                " <condition entityname = 'new_chinhsachthumua' attribute='new_hoatdongapdung' operator='eq' value='100000003' /> " +
                                " <condition entityname = 'new_chinhsachthumua' attribute='statecode' operator='eq' value='0' /> " +
                            " </filter> " +
                            " </entity> " +
                        " </fetch> ";
            FetchExpression qe = new FetchExpression(query);
            return service.RetrieveMultiple(qe);
        }

        Guid CreatePTTM(Entity lenhdon, Entity CS)
        {
            Entity tmp = new Entity("new_phieutinhtienmia");
            tmp["new_name"] = "PTTM_" + lenhdon["new_name"].ToString();
            tmp["new_vudautu"] = lenhdon["new_vudautu"];
            if (lenhdon.Contains("new_khachhang"))
                tmp["new_khachhang"] = lenhdon["new_khachhang"];
            else tmp["new_khachhangdoanhnghiep"] = lenhdon["new_khachhangdoanhnghiep"];
            tmp["new_chinhsachthumua"] = CS.ToEntityReference();
            if (lenhdon.Contains("new_hopdongdautumia"))
            {
                tmp["new_hopdongdautumia"] = lenhdon["new_hopdongdautumia"];
                tmp["new_thuacanhtac"] = lenhdon["new_thuacanhtac"];
            }
            if (lenhdon.Contains("new_hopdongmuabanmiangoai"))
                tmp["new_hopdongmuabanmiangoai"] = lenhdon["new_hopdongmuabanmiangoai"];
            if (lenhdon.Contains("new_hopdongvanchuyen"))
                tmp["new_hopdongvanchuyen"] = lenhdon["new_hopdongvanchuyen"];
            if (lenhdon.Contains("new_hopdongthuhoach"))
                tmp["new_hopdongthuhoach"] = lenhdon["new_hopdongthuhoach"];
            tmp["new_lenhdon"] = lenhdon.ToEntityReference();
            tmp["new_bangke"] = target;
            if (lenhdon.Contains("new_doitacthuhoach"))
                tmp["new_doitacthuhoach"] = lenhdon["new_doitacthuhoach"];
            if (lenhdon.Contains("new_doitacthuhoachkhdn"))
                tmp["new_doitacthuhoachkhdn"] = lenhdon["new_doitacthuhoachkhdn"];
            if (lenhdon.Contains("new_doitacvanchuyen"))
                tmp["new_doitacvanchuyen"] = lenhdon["new_doitacvanchuyen"];
            if (lenhdon.Contains("new_doitacvanchuyenkhdn"))
                tmp["new_doitacvanchuyenkhdn"] = lenhdon["new_doitacvanchuyenkhdn"];

            //Tính tạp chất thanh toán tapchattru  - Tạp chất trừ
            decimal phattapchatcao = 0;
            int nguontienthuongtapchat = 100000000; //từ tiền mía
            decimal thuongtapchat = 0;
            decimal thuongccs = 0;
            decimal tapchatthanhtoan = (decimal)lenhdon["new_tapchatthucte"];

            #region check phạt tạp chất
            if (CS.Contains("tapchattru"))
                foreach (Entity tc in ((EntityCollection)CS["tapchattru"]).Entities)
                {
                    string tu = "";
                    switch (((OptionSetValue)tc["new_operatortu"]).Value)
                    {
                        case 100000000:
                            tu = " = ";
                            break;
                        case 100000001:
                            tu = " < ";
                            break;
                        case 100000002:
                            tu = " > ";
                            break;
                        case 100000003:
                            tu = " <= ";
                            break;
                        case 100000004:
                            tu = " >= ";
                            break;
                        default:
                            tu = " = ";
                            break;
                    }
                    string den = "";
                    switch (((OptionSetValue)tc["new_operatorden"]).Value)
                    {
                        case 100000000:
                            den = " = ";
                            break;
                        case 100000001:
                            den = " < ";
                            break;
                        case 100000002:
                            den = " > ";
                            break;
                        case 100000003:
                            den = " <= ";
                            break;
                        case 100000004:
                            den = " >= ";
                            break;
                        default:
                            den = " = ";
                            break;
                    }
                    string express = string.Format("({0} {1} {2}) and ({3} {4} {5})", tapchatthanhtoan, tu, (decimal)tc["new_tu"], tapchatthanhtoan, den, (decimal)tc["new_den"]);
                    if (Evaluate(express))
                    {
                        //tính tạp chất
                        switch (((OptionSetValue)tc["new_cachtru"]).Value)
                        {
                            case 100000000: //Miển trừ
                                tapchatthanhtoan = 0;
                                break;
                            case 100000001: // phần dư
                                tapchatthanhtoan = tapchatthanhtoan - (decimal)tc["new_tu"];
                                break;
                            case 100000003: //Nhân hệ số
                                tapchatthanhtoan = tapchatthanhtoan * (decimal)tc["new_heso"];
                                break;
                            case 100000004: //Phạt tạp chất cao
                                phattapchatcao = ((Money)tc["new_sotienphat"]).Value;
                                break;
                        }
                        break;
                    }
                }
            #endregion

            //Tính ccs thanh toán ccsbao  ccsbao_ths  - CCS bao
            decimal ccsThanhtoan = (decimal)lenhdon["new_ccsthucte"];
            Entity bbThuHoachSom = null;
            DSBBThuHoachSom.TryGetValue(((EntityReference)lenhdon["new_thuacanhtac"]).Id, out bbThuHoachSom);

            if (bbThuHoachSom != null && (bool)bbThuHoachSom["new_baohiemccs"] && (DateTime)lenhdon["new_ngaycap"] <= (DateTime)bbThuHoachSom["new_ngayhethan"] && !(bool)lenhdon["new_miadutuoi"])
            {
                #region tinhccsthanhtoan_thuhoachsom
                if (CS.Contains("ccsbao_ths"))
                    foreach (Entity ccs in ((EntityCollection)CS["ccsbao_ths"]).Entities)
                    {
                        string tu = "";
                        switch (((OptionSetValue)ccs["new_operatortu"]).Value)
                        {
                            case 100000000:
                                tu = " = ";
                                break;
                            case 100000001:
                                tu = " < ";
                                break;
                            case 100000002:
                                tu = " > ";
                                break;
                            case 100000003:
                                tu = " <= ";
                                break;
                            case 100000004:
                                tu = " >= ";
                                break;
                            default:
                                tu = " = ";
                                break;
                        }
                        string den = "";
                        switch (((OptionSetValue)ccs["new_operatorden"]).Value)
                        {
                            case 100000000:
                                den = " = ";
                                break;
                            case 100000001:
                                den = " < ";
                                break;
                            case 100000002:
                                den = " > ";
                                break;
                            case 100000003:
                                den = " <= ";
                                break;
                            case 100000004:
                                den = " >= ";
                                break;
                            default:
                                den = " = ";
                                break;
                        }
                        string express = string.Format("({0} {1} {2}) and ({3} {4} {5})", ccsThanhtoan, tu, (decimal)ccs["new_tu"], ccsThanhtoan, den, (decimal)ccs["new_den"]);
                        if (Evaluate(express))
                        {
                            ccsThanhtoan = (decimal)ccs["new_giatriccs"];
                            break;
                        }
                    }
                #endregion
            }
            else
            {
                #region tinhccsthanhtoan
                if (CS.Contains("ccsbao"))
                    foreach (Entity ccs in ((EntityCollection)CS["ccsbao"]).Entities)
                    {
                        string tu = "";
                        switch (((OptionSetValue)ccs["new_operatortu"]).Value)
                        {
                            case 100000000:
                                tu = " = ";
                                break;
                            case 100000001:
                                tu = " < ";
                                break;
                            case 100000002:
                                tu = " > ";
                                break;
                            case 100000003:
                                tu = " <= ";
                                break;
                            case 100000004:
                                tu = " >= ";
                                break;
                            default:
                                tu = " = ";
                                break;
                        }
                        string den = "";
                        switch (((OptionSetValue)ccs["new_operatorden"]).Value)
                        {
                            case 100000000:
                                den = " = ";
                                break;
                            case 100000001:
                                den = " < ";
                                break;
                            case 100000002:
                                den = " > ";
                                break;
                            case 100000003:
                                den = " <= ";
                                break;
                            case 100000004:
                                den = " >= ";
                                break;
                            default:
                                den = " = ";
                                break;
                        }
                        string express = string.Format("({0} {1} {2}) and ({3} {4} {5})", ccsThanhtoan, tu, (decimal)ccs["new_tu"], ccsThanhtoan, den, (decimal)ccs["new_den"]);
                        if (Evaluate(express))
                        {
                            ccsThanhtoan = (decimal)ccs["new_giatriccs"];
                            break;
                        }
                    }
                #endregion
            }

            //Tạp chất thưởng tapchatthuong
            #region tinh tap chat thuong
            if (CS.Contains("tapchatthuong"))
            {
                foreach (Entity tc in ((EntityCollection)CS["tapchatthuong"]).Entities)
                {
                    string tu = "";
                    switch (((OptionSetValue)tc["new_operatortu"]).Value)
                    {
                        case 100000000:
                            tu = " = ";
                            break;
                        case 100000001:
                            tu = " < ";
                            break;
                        case 100000002:
                            tu = " > ";
                            break;
                        case 100000003:
                            tu = " <= ";
                            break;
                        case 100000004:
                            tu = " >= ";
                            break;
                        default:
                            tu = " = ";
                            break;
                    }
                    string den = "";
                    switch (((OptionSetValue)tc["new_operatorden"]).Value)
                    {
                        case 100000000:
                            den = " = ";
                            break;
                        case 100000001:
                            den = " < ";
                            break;
                        case 100000002:
                            den = " > ";
                            break;
                        case 100000003:
                            den = " <= ";
                            break;
                        case 100000004:
                            den = " >= ";
                            break;
                        default:
                            den = " = ";
                            break;
                    }
                    string express = string.Format("({0} {1} {2}) and ({3} {4} {5})", (decimal)lenhdon["new_tapchatthucte"], tu, (decimal)tc["new_tu"], (decimal)lenhdon["new_tapchatthucte"], den, (decimal)tc["new_den"]);
                    if (Evaluate(express))
                    {
                        thuongtapchat = ((Money)tc["new_tienthuong"]).Value;
                        nguontienthuongtapchat = ((OptionSetValue)tc["new_nguontien"]).Value;
                        break;
                    }
                }
            }

            #endregion

            //CCS thưởng ccsthuong
            #region tinh ccs thuong
            if (CS.Contains("ccsthuong"))
            {
                foreach (Entity tc in ((EntityCollection)CS["ccsthuong"]).Entities)
                {
                    string tu = "";
                    switch (((OptionSetValue)tc["new_operatortu"]).Value)
                    {
                        case 100000000:
                            tu = " = ";
                            break;
                        case 100000001:
                            tu = " < ";
                            break;
                        case 100000002:
                            tu = " > ";
                            break;
                        case 100000003:
                            tu = " <= ";
                            break;
                        case 100000004:
                            tu = " >= ";
                            break;
                        default:
                            tu = " = ";
                            break;
                    }
                    string den = "";
                    switch (((OptionSetValue)tc["new_operatorden"]).Value)
                    {
                        case 100000000:
                            den = " = ";
                            break;
                        case 100000001:
                            den = " < ";
                            break;
                        case 100000002:
                            den = " > ";
                            break;
                        case 100000003:
                            den = " <= ";
                            break;
                        case 100000004:
                            den = " >= ";
                            break;
                        default:
                            den = " = ";
                            break;
                    }
                    string express = string.Format("({0} {1} {2}) and ({3} {4} {5})", (decimal)lenhdon["new_ccsthucte"], tu, (decimal)tc["new_tu"], (decimal)lenhdon["new_ccsthucte"], den, (decimal)tc["new_den"]);
                    if (Evaluate(express))
                    {
                        thuongccs = ((Money)tc["new_tienthuong"]).Value;
                        break;
                    }
                }
            }
            #endregion

            decimal klmia = ((decimal)lenhdon["new_trongluongxoi"] - (decimal)lenhdon["new_trongluongbi"]);
            decimal klthanhtoan = klmia * (1 - (tapchatthanhtoan / 100));
            decimal giamia = 0;
            decimal dongiahotrogiamia = CS.Contains("new_dongiahotromuamia") ? ((Money)CS["new_dongiahotromuamia"]).Value : (decimal)0;
            decimal dongiahotrocongdon = CS.Contains("new_dongiahotrothuhoach") ? ((Money)CS["new_dongiahotrothuhoach"]).Value : (decimal)0;

            decimal tienmia = 0;
            decimal giacongdon = 0;
            decimal tiencongdon = 0;
            decimal giavanchuyen = 0;
            decimal tienvanchuyen = 0;

            decimal tienthuongccs = klthanhtoan * thuongccs;
            decimal tienthuongtapchat = klthanhtoan * thuongtapchat;
            decimal tienphattapchat = klthanhtoan * phattapchatcao;

            decimal tamgiuchumia = 0;
            decimal tamgiucongdon = 0;
            decimal tamgiuvanchuyen = 0;

            decimal tienchichumia = 0;
            decimal tienchicongdon = 0;
            decimal tienchivanchuyen = 0;

            //Tính tiền mía
            if (((OptionSetValue)CS["new_hoatdongapdung"]).Value == 100000000) //HD Đầu tư Mía
            {
                // tính tiền mía
                decimal muctanggiam = ((ccsThanhtoan - 10 >= 0) ? (CS.Contains("new_dongiatang1ccs") ? ((Money)CS["new_dongiatang1ccs"]).Value : (decimal)0) :
                    CS.Contains("new_dongiagiam1ccs") ? ((Money)CS["new_dongiagiam1ccs"]).Value : (decimal)0);
                giamia = ((Money)CS["new_dongiamiacobantairuong"]).Value + (muctanggiam * (ccsThanhtoan - 10));
                tmp["new_giamia"] = new Money(giamia);
                tmp["new_tienmia"] = new Money(klthanhtoan * giamia);
                giamia += dongiahotrogiamia;
                tienmia = klthanhtoan * giamia;

                //Tính tiền công đốn
                if (lenhdon.Contains("new_hopdongthuhoach"))
                {
                    foreach (Entity giacong in DSTTCongDon.Entities.Where(o => ((EntityReference)o["new_thuadatcanhtac"]).Id == ((EntityReference)lenhdon["new_thuacanhtac"]).Id))
                    {
                        if (giacong.Contains("b.new_daucongkh"))
                        {
                            if (((EntityReference)lenhdon["new_doitacthuhoach"]).Id == ((EntityReference)((AliasedValue)giacong["b.new_daucongkh"]).Value).Id)
                            {
                                if (!(bool)lenhdon["new_miachay"]) //mía tươi
                                    giacongdon = (giacong.Contains("new_congdonchatvabocmia") ? ((Money)giacong["new_congdonchatvabocmia"]).Value : (decimal)0) + (giacong.Contains("new_trungchuyen") ? ((Money)giacong["new_trungchuyen"]).Value : (decimal)0);
                                else
                                {
                                    giacongdon = (giacong.Contains("new_giacongmiachay") ? ((Money)giacong["new_giacongmiachay"]).Value : (decimal)0) + (giacong.Contains("new_trungchuyen") ? ((Money)giacong["new_trungchuyen"]).Value : (decimal)0);
                                }
                                break;
                            }
                        }

                        if (giacong.Contains("b.new_daucong2kh"))
                        {
                            if (((EntityReference)lenhdon["new_doitacthuhoach"]).Id == ((EntityReference)((AliasedValue)giacong["b.new_daucong2kh"]).Value).Id)
                            {
                                if (!(bool)lenhdon["new_miachay"]) //mía tươi
                                    giacongdon = (giacong.Contains("new_congdonchatvabocmia") ? ((Money)giacong["new_congdonchatvabocmia"]).Value : (decimal)0) + (giacong.Contains("new_trungchuyen") ? ((Money)giacong["new_trungchuyen"]).Value : (decimal)0);
                                else
                                {
                                    giacongdon = (giacong.Contains("new_giacongmiachay") ? ((Money)giacong["new_giacongmiachay"]).Value : (decimal)0) + (giacong.Contains("new_trungchuyen") ? ((Money)giacong["new_trungchuyen"]).Value : (decimal)0);
                                }
                                break;
                            }
                        }
                        if (giacong.Contains("b.new_daucongkhdn"))
                        {
                            if (((EntityReference)lenhdon["new_doitacthuhoachkhdn"]).Id == ((EntityReference)((AliasedValue)giacong["b.new_daucongkhdn"]).Value).Id)
                            {
                                if (!(bool)lenhdon["new_miachay"]) //mía tươi
                                    giacongdon = (giacong.Contains("new_congdonchatvabocmia") ? ((Money)giacong["new_congdonchatvabocmia"]).Value : (decimal)0) + (giacong.Contains("new_trungchuyen") ? ((Money)giacong["new_trungchuyen"]).Value : (decimal)0);
                                else
                                {
                                    giacongdon = (giacong.Contains("new_giacongmiachay") ? ((Money)giacong["new_giacongmiachay"]).Value : (decimal)0) + (giacong.Contains("new_trungchuyen") ? ((Money)giacong["new_trungchuyen"]).Value : (decimal)0);
                                }
                                break;
                            }
                        }
                        if (giacong.Contains("b.new_daucong2khdn"))
                        {
                            if (((EntityReference)lenhdon["new_doitacthuhoachkhdn"]).Id == ((EntityReference)((AliasedValue)giacong["b.new_daucong2khdn"]).Value).Id)
                            {
                                if (!(bool)lenhdon["new_miachay"]) //mía tươi
                                    giacongdon = (giacong.Contains("new_congdonchatvabocmia") ? ((Money)giacong["new_congdonchatvabocmia"]).Value : (decimal)0) + (giacong.Contains("new_trungchuyen") ? ((Money)giacong["new_trungchuyen"]).Value : (decimal)0);
                                else
                                {
                                    giacongdon = (giacong.Contains("new_giacongmiachay") ? ((Money)giacong["new_giacongmiachay"]).Value : (decimal)0) + (giacong.Contains("new_trungchuyen") ? ((Money)giacong["new_trungchuyen"]).Value : (decimal)0);
                                }
                                break;
                            }
                        }
                    }
                }
                else
                {
                    if (((OptionSetValue)VuDauTu["new_loaitrichthu"]).Value == 100000000)
                        giacongdon = (decimal)VuDauTu["new_giatri"];
                    else
                        giacongdon = tienmia * (decimal)VuDauTu["new_giatri"] / 100;
                }
                tiencongdon = giacongdon * klthanhtoan;

                //Tính tiền vận chuyển
                foreach (Entity gvc in BangGiaVanChuyen.Entities.Where(o => ((EntityReference)((AliasedValue)o["a.new_vungdialy"]).Value).Id == ((EntityReference)((AliasedValue)lenhdon["i.new_vungdialy"]).Value).Id &&
                                                                            Decimal.ToInt32((decimal)((AliasedValue)o["a.new_culy"]).Value) == Decimal.ToInt32((decimal)((AliasedValue)lenhdon["b.new_culy"]).Value)).OrderByDescending(o => (DateTime)((AliasedValue)o["a.new_ngayapdung"]).Value))
                {
                    giavanchuyen = gvc.Contains("a.new_giacuoc") ? ((Money)((AliasedValue)gvc["a.new_giacuoc"]).Value).Value : (decimal)0;
                    break;
                }
                tienvanchuyen = giavanchuyen * klthanhtoan;

                //tính tiền tạm giữ chủ mía
                if (bbThuHoachSom != null && (bool)bbThuHoachSom["new_tamgiu"] && !(bool)lenhdon["new_miadutuoi"])
                {
                    decimal tamgiutoida = 0;
                    if (lenhdon.Contains("b.new_tamgiutoida"))
                        tamgiutoida = ((Money)((AliasedValue)lenhdon["b.new_tamgiutoida"]).Value).Value;

                    decimal datamgiu = 0;
                    if (TamgiutheoThua.ContainsKey(((EntityReference)lenhdon["new_thuacanhtac"]).Id))
                        datamgiu = TamgiutheoThua[((EntityReference)lenhdon["new_thuacanhtac"]).Id];
                    else
                        TamgiutheoThua.Add(((EntityReference)lenhdon["new_thuacanhtac"]).Id, (decimal)0);

                    //tienmia + hỗ trợ + thưởng ccs - thưởng tạp chất trích từ tiền mía.
                    decimal tienmiatmp = tienmia + tienthuongccs - (nguontienthuongtapchat == 100000000 ? tienthuongtapchat : (decimal)0);

                    switch (((OptionSetValue)((AliasedValue)lenhdon["b.new_donvitinh"]).Value).Value)
                    {
                        case 100000000: //Diện tích - số tiền (đ/ha) 
                            decimal dientich = bbThuHoachSom.Contains("new_dientichthsom") ? (decimal)bbThuHoachSom["new_dientichthsom"] : (decimal)((AliasedValue)lenhdon["b.new_dientichconlai"]).Value;
                            decimal tmptamgiu = (decimal)((AliasedValue)lenhdon["b.new_giatritamgiu"]).Value * dientich;
                            if (tmptamgiu < tamgiutoida)
                                tamgiutoida = tmptamgiu;
                            if (tamgiutoida > 0)
                            {
                                tamgiuchumia = ((tienmiatmp - tiencongdon) <= (tamgiutoida - datamgiu) ? tienmiatmp - tiencongdon : tamgiutoida - datamgiu);
                                TamgiutheoThua[((EntityReference)lenhdon["new_thuacanhtac"]).Id] += tamgiuchumia;
                            }
                            break;
                        default: //Số tiền (tấn)
                            if (((OptionSetValue)((AliasedValue)lenhdon["b.new_cachtinhtamgiu"]).Value).Value == 100000000) //Tính theo số tiền
                            {
                                decimal tmptamgiu1 = (decimal)((AliasedValue)lenhdon["b.new_giatritamgiu"]).Value * klthanhtoan;
                                if (tamgiutoida > 0)
                                {
                                    tamgiuchumia = ((tienmiatmp - tiencongdon) <= tmptamgiu1 ? tienmiatmp - tiencongdon : tmptamgiu1);
                                    tamgiuchumia = ((tamgiutoida - datamgiu) <= tmptamgiu1 ? (tamgiutoida - datamgiu) : tamgiuchumia);
                                }
                                else
                                {
                                    tamgiuchumia = tmptamgiu1;
                                }
                                TamgiutheoThua[((EntityReference)lenhdon["new_thuacanhtac"]).Id] += tamgiuchumia;
                            }
                            else //theo tỷ lệ %
                            {
                                decimal tmptamgiu2 = (decimal)((AliasedValue)lenhdon["b.new_giatritamgiu"]).Value / 100 * (tienmiatmp - tiencongdon);
                                if (tamgiutoida > 0)
                                {
                                    tamgiuchumia = ((tienmiatmp - tiencongdon) <= tmptamgiu2 ? tienmiatmp - tiencongdon : tmptamgiu2);
                                    tamgiuchumia = ((tamgiutoida - datamgiu) <= tmptamgiu2 ? (tamgiutoida - datamgiu) : tamgiuchumia);
                                }
                                else
                                {
                                    tamgiuchumia = tmptamgiu2;
                                }
                                TamgiutheoThua[((EntityReference)lenhdon["new_thuacanhtac"]).Id] += tamgiuchumia;
                            }
                            break;
                    }
                }

                //tính tạm giữ công đốn
                if (lenhdon.Contains("new_hopdongthuhoach") && lenhdon.Contains("hdth.new_dinhmuctientamgiu"))
                {
                    decimal tamgiutoida = 0;
                    if (lenhdon.Contains("hdth.new_tamgiutoida"))
                        tamgiutoida = ((Money)((AliasedValue)lenhdon["hdth.new_tamgiutoida"]).Value).Value;

                    decimal datamgiu = 0;
                    if (TamgiutheoHDTH.ContainsKey(((EntityReference)lenhdon["new_hopdongthuhoach"]).Id))
                        datamgiu = TamgiutheoHDTH[((EntityReference)lenhdon["new_hopdongthuhoach"]).Id];
                    else
                        TamgiutheoHDTH.Add(((EntityReference)lenhdon["new_hopdongthuhoach"]).Id, (decimal)0);

                    decimal tmptamgiu = ((Money)((AliasedValue)lenhdon["hdth.new_dinhmuctientamgiu"]).Value).Value * klthanhtoan;

                    //tiền công đốn + hỗ trợ thu hoạch + thưởng tạp chất
                    decimal tiencongdontmp = tiencongdon + (dongiahotrocongdon * klthanhtoan) + tienthuongtapchat - tienphattapchat;

                    if (tamgiutoida > 0)
                    {
                        tamgiucongdon = (tiencongdontmp <= tmptamgiu ? tiencongdontmp : tmptamgiu);
                        tamgiucongdon = ((tamgiutoida - datamgiu) <= tmptamgiu ? (tamgiutoida - datamgiu) : tamgiucongdon);
                    }
                    else tamgiucongdon = tmptamgiu;
                    TamgiutheoHDTH[((EntityReference)lenhdon["new_hopdongthuhoach"]).Id] += tamgiucongdon;
                }

                //tính tạm giữ vận chuyển
                if (lenhdon.Contains("new_hopdongvanchuyen") && lenhdon.Contains("hdvc.new_giatritamgiu"))
                {
                    decimal tamgiutoida = 0;
                    if (lenhdon.Contains("hdvc.new_tamgiutoida"))
                        tamgiutoida = ((Money)((AliasedValue)lenhdon["hdvc.new_tamgiutoida"]).Value).Value;

                    decimal datamgiu = 0;
                    if (TamgiutheoHDVC.ContainsKey(((EntityReference)lenhdon["new_hopdongvanchuyen"]).Id))
                        datamgiu = TamgiutheoHDVC[((EntityReference)lenhdon["new_hopdongvanchuyen"]).Id];
                    else
                        TamgiutheoHDVC.Add(((EntityReference)lenhdon["new_hopdongvanchuyen"]).Id, (decimal)0);

                    //tiền vạn chuyển + hỗ trợ vận chuyển
                    decimal tienvanchuyentmp = tienvanchuyen;

                    switch (((OptionSetValue)((AliasedValue)lenhdon["hdvc.new_donvitinh"]).Value).Value)
                    {
                        case 100000000: // Theo chuyến
                            if (((OptionSetValue)((AliasedValue)lenhdon["hdvc.new_cachtinhtamgiu"]).Value).Value == 100000000) //Tính theo số tiền
                            {
                                decimal tmptamgiu1 = (decimal)((AliasedValue)lenhdon["hdvc.new_giatritamgiu"]).Value;
                                if (tamgiutoida > 0)
                                {
                                    tamgiuvanchuyen = (tienvanchuyentmp <= tmptamgiu1 ? tienvanchuyentmp : tmptamgiu1);
                                    tamgiuvanchuyen = ((tamgiutoida - datamgiu) <= tmptamgiu1 ? (tamgiutoida - datamgiu) : tamgiuvanchuyen);
                                }
                                else
                                {
                                    tamgiuvanchuyen = tmptamgiu1;
                                }
                                TamgiutheoHDVC[((EntityReference)lenhdon["new_hopdongvanchuyen"]).Id] += tamgiuvanchuyen;
                            }
                            else //theo tỷ lệ %
                            {
                                decimal tmptamgiu2 = (decimal)((AliasedValue)lenhdon["hdvc.new_giatritamgiu"]).Value / 100 * tienvanchuyentmp;
                                if (tamgiutoida > 0)
                                {
                                    tamgiuvanchuyen = (tienvanchuyentmp <= tmptamgiu2 ? tienvanchuyentmp : tmptamgiu2);
                                    tamgiuvanchuyen = ((tamgiutoida - datamgiu) <= tmptamgiu2 ? (tamgiutoida - datamgiu) : tamgiuvanchuyen);
                                }
                                else
                                {
                                    tamgiuvanchuyen = tmptamgiu2;
                                }
                                TamgiutheoHDVC[((EntityReference)lenhdon["new_hopdongvanchuyen"]).Id] += tamgiuvanchuyen;
                            }
                            break;
                        default: // Sản lượng (tấn)
                            decimal tmptamgiu = (decimal)((AliasedValue)lenhdon["hdvc.new_giatritamgiu"]).Value * klthanhtoan;
                            if (tamgiutoida > 0)
                            {
                                tamgiuvanchuyen = (tienvanchuyentmp <= tmptamgiu ? tienvanchuyentmp : tmptamgiu);
                                tamgiuvanchuyen = ((tamgiutoida - datamgiu) <= tmptamgiu ? (tamgiutoida - datamgiu) : tamgiuvanchuyen);
                            }
                            else tamgiuvanchuyen = tmptamgiu;
                            TamgiutheoHDVC[((EntityReference)lenhdon["new_hopdongvanchuyen"]).Id] += tamgiuvanchuyen;

                            break;
                    }
                }

                //Tính nợ nông dân
                tienchichumia = tienmia + tienthuongccs - (nguontienthuongtapchat == 100000000 ? tienthuongtapchat : (decimal)0) - tamgiuchumia - tiencongdon;

                decimal thugocchumia = 0;
                decimal thulaichumia = 0;
                Guid PDNThuNoChuMia = Guid.Empty;
                //if (lenhdon.Contains("new_khachhang"))
                //    PDNThuNoChuMia = Noservices.Run(VuDauTu.Id, "contact", ((EntityReference)lenhdon["new_khachhang"]).Id, tienchichumia, 0, Guid.Empty, ((EntityReference)lenhdon["new_hopdongdautumia"]).Id,
                //         ((EntityReference)lenhdon["new_thuacanhtac"]).Id, (DateTime)lenhdon["new_thoigiancanra"], ref thugocchumia, ref thulaichumia);
                //else
                //    PDNThuNoChuMia = Noservices.Run(VuDauTu.Id, "account", ((EntityReference)lenhdon["new_khachhangdoanhnghiep"]).Id, tienchichumia, 0, Guid.Empty, ((EntityReference)lenhdon["new_hopdongdautumia"]).Id,
                //         ((EntityReference)lenhdon["new_thuacanhtac"]).Id, (DateTime)lenhdon["new_thoigiancanra"], ref thugocchumia, ref thulaichumia);
                tienchichumia = tienchichumia - thugocchumia - thulaichumia;

                //Tính nợ thu hoạch
                if (lenhdon.Contains("new_hopdongthuhoach"))
                {
                    tienchicongdon = tiencongdon + (dongiahotrocongdon * klthanhtoan) + tienthuongtapchat - tamgiucongdon - tienphattapchat;

                    decimal thugocthuhoach = 0;
                    decimal thulaithuhoach = 0;
                    Guid PDNThuNoThuHoach = Guid.Empty;
                    //if (lenhdon.Contains("new_doitacthuhoach"))
                    //    PDNThuNoThuHoach = Noservices.Run(VuDauTu.Id, "contact", ((EntityReference)lenhdon["new_doitacthuhoach"]).Id, tienchicongdon * (lenhdon.Contains("hdth.new_tylethuhoi") ? (decimal)((AliasedValue)lenhdon["hdth.new_tylethuhoi"]).Value : (decimal)100) / 100, 1,
                    //        Guid.Empty, Guid.Empty, Guid.Empty, (DateTime)lenhdon["new_thoigiancanra"], ref thugocthuhoach, ref thulaithuhoach);
                    //else
                    //    PDNThuNoThuHoach = Noservices.Run(VuDauTu.Id, "account", ((EntityReference)lenhdon["new_doitacthuhoachkhdn"]).Id, tienchicongdon * (lenhdon.Contains("hdth.new_tylethuhoi") ? (decimal)((AliasedValue)lenhdon["hdth.new_tylethuhoi"]).Value : (decimal)100) / 100, 1,
                    //        Guid.Empty, Guid.Empty, Guid.Empty, (DateTime)lenhdon["new_thoigiancanra"], ref thugocthuhoach, ref thulaithuhoach);
                    tienchicongdon = tienchicongdon - thugocthuhoach - thulaithuhoach;
                    if (PDNThuNoThuHoach != Guid.Empty)
                    {
                        tmp["new_pdnthuno_thuhoach"] = new EntityReference("new_phieudenghithuno", PDNThuNoThuHoach);
                        tmp["new_tienlai_thuhoach"] = new Money(thulaithuhoach);
                        tmp["new_trugoc_thuhoach"] = new Money(thugocthuhoach);
                    }
                }
                else
                {
                    tienchichumia += (tiencongdon + (dongiahotrocongdon * klthanhtoan) + tienthuongtapchat - tamgiucongdon - tienphattapchat);
                }

                if (lenhdon.Contains("new_hopdongvanchuyen"))
                {
                    //Tính nợ vận chuyển
                    tienchivanchuyen = tienvanchuyen - tamgiuvanchuyen;

                    decimal thugocvanchuyen = 0;
                    decimal thulaivanchuyen = 0;
                    Guid PDNThuNoVanChuyen = Guid.Empty;
                    //if (lenhdon.Contains("new_doitacvanchuyen"))
                    //    PDNThuNoVanChuyen = Noservices.Run(VuDauTu.Id, "contact", ((EntityReference)lenhdon["new_doitacvanchuyen"]).Id, tienchivanchuyen * (lenhdon.Contains("hdvc.new_tylethuhoi") ? (decimal)((AliasedValue)lenhdon["hdvc.new_tylethuhoi"]).Value : (decimal)100) / 100, 1,
                    //         Guid.Empty, Guid.Empty, Guid.Empty, (DateTime)lenhdon["new_thoigiancanra"], ref thugocvanchuyen, ref thulaivanchuyen);
                    //else
                    //    PDNThuNoVanChuyen = Noservices.Run(VuDauTu.Id, "account", ((EntityReference)lenhdon["new_doitacvanchuyenkhdn"]).Id, tienchivanchuyen * (lenhdon.Contains("hdvc.new_tylethuhoi") ? (decimal)((AliasedValue)lenhdon["hdvc.new_tylethuhoi"]).Value : (decimal)100) / 100, 1,
                    //         Guid.Empty, Guid.Empty, Guid.Empty, (DateTime)lenhdon["new_thoigiancanra"], ref thugocvanchuyen, ref thulaivanchuyen);
                    tienchivanchuyen = tienchivanchuyen - thugocvanchuyen - thulaivanchuyen;
                    if (PDNThuNoVanChuyen != Guid.Empty)
                    {
                        tmp["new_pdnthuno_vanchuyen"] = new EntityReference("new_phieudenghithuno", PDNThuNoVanChuyen);
                        tmp["new_tienlai_vanchuyen"] = new Money(thulaivanchuyen);
                        tmp["new_trugoc_vanchuyen"] = new Money(thugocvanchuyen);
                    }
                }
                else
                {
                    tienchichumia += (tienvanchuyen - tamgiuvanchuyen);
                }

                //tien mia + Thưởng ccs + thưởng tạp chất + Hỗ trợ mua mía + hỗ trợ thu hoạch + tiền vận chuyển - phạt tạp chất.
                decimal chiphimuamia = tienmia + tienthuongccs + (nguontienthuongtapchat == 100000000 ? (decimal)0 : tienthuongtapchat)
                    + (dongiahotrocongdon * klthanhtoan) + tienvanchuyen - tienphattapchat;

                //Input data

                tmp["new_giathuhoach"] = new Money(giacongdon);
                tmp["new_giavanchuyen"] = new Money(giavanchuyen);
                tmp["new_khoiluongthanhtoan"] = klthanhtoan;
                tmp["new_tienxe"] = new Money(tienvanchuyen);
                tmp["new_tiencongdon"] = new Money(tiencongdon);

                tmp["new_tienthuongccs"] = new Money(tienthuongccs);
                tmp["new_tienthuongtapchat"] = new Money(tienthuongtapchat);
                tmp["new_tienphattapchat"] = new Money(tienphattapchat);

                tmp["new_hotromuamia"] = new Money(klthanhtoan * dongiahotrogiamia);
                tmp["new_hotrothuhoach"] = new Money(klthanhtoan * dongiahotrocongdon);

                tmp["new_tamgiunongdan"] = new Money(tamgiuchumia);
                tmp["new_tamgiuthuhoach"] = new Money(tamgiucongdon);
                tmp["new_tamgiuvanchuyen"] = new Money(tamgiuvanchuyen);

                tmp["new_datinhtienmia"] = true;
                tmp["new_datinhtienkhuyenkhich"] = false;

                tmp["new_chiphinhamaymuamia"] = new Money(chiphimuamia);

                tmp["new_tienchidaucong"] = new Money(tienchicongdon);
                tmp["new_tienchivanchuyen"] = new Money(tienchivanchuyen);
                tmp["new_tienchichumia"] = new Money(tienchichumia);

                if (PDNThuNoChuMia != Guid.Empty)
                {
                    tmp["new_pdnthuno"] = new EntityReference("new_phieudenghithuno", PDNThuNoChuMia);
                    tmp["new_tienlai"] = new Money(thulaichumia);
                    tmp["new_trugoc"] = new Money(thugocchumia);
                }
                tmp["new_tapchatthanhtoan"] = tapchatthanhtoan;
                tmp["new_ccsthanhtoan"] = ccsThanhtoan;
                tmp["new_khoiluongthanhtoan"] = klthanhtoan;
            }
            else { //HĐ mua mía ngoài
                //Tính tiền vận chuyển
                foreach (Entity gvc in BangGiaVanChuyen.Entities.Where(o => ((EntityReference)((AliasedValue)o["a.new_vungdialy"]).Value).Id == ((EntityReference)((AliasedValue)lenhdon["i.new_vungdialy"]).Value).Id &&
                                                                            Decimal.ToInt32((decimal)((AliasedValue)o["a.new_culy"]).Value) == Decimal.ToInt32((decimal)((AliasedValue)lenhdon["b.new_culy"]).Value)).OrderByDescending(o => (DateTime)((AliasedValue)o["a.new_ngayapdung"]).Value))
                {
                    giavanchuyen = gvc.Contains("a.new_giacuoc") ? ((Money)((AliasedValue)gvc["a.new_giacuoc"]).Value).Value : (decimal)0;
                    break;
                }
                tienvanchuyen = giavanchuyen * klthanhtoan;

                // tính tiền mía
                decimal muctanggiam = ((ccsThanhtoan - 10 >= 0) ? (CS.Contains("new_dongiatang1ccs") ? ((Money)CS["new_dongiatang1ccs"]).Value : (decimal)0) :
                     CS.Contains("new_dongiagiam1ccs") ? ((Money)CS["new_dongiagiam1ccs"]).Value : (decimal)0);
                giamia = ((Money)CS["new_dongiamiacobantairuong"]).Value + (muctanggiam * (ccsThanhtoan - 10));
                tmp["new_giamia"] = new Money(giamia);
                tmp["new_tienmia"] = new Money(klthanhtoan * giamia);

                giamia += dongiahotrogiamia;
                tienmia = klthanhtoan * giamia;

                tienchichumia = tienmia + tienthuongccs + tienthuongtapchat - (nguontienthuongtapchat == 100000000 ? tienthuongtapchat : (decimal)0) - tienphattapchat;

                decimal thugocchumia = 0;
                decimal thulaichumia = 0;
                Guid PDNThuNoChuMia = Guid.Empty;
                //if (lenhdon.Contains("new_khachhang"))
                //    PDNThuNoChuMia = Noservices.Run(VuDauTu.Id, "contact", ((EntityReference)lenhdon["new_khachhang"]).Id, tienchichumia, 0, Guid.Empty, ((EntityReference)lenhdon["new_hopdongdautumia"]).Id,
                //         ((EntityReference)lenhdon["new_thuacanhtac"]).Id, (DateTime)lenhdon["new_thoigiancanra"], ref thugocchumia, ref thulaichumia);
                //else
                //    PDNThuNoChuMia = Noservices.Run(VuDauTu.Id, "account", ((EntityReference)lenhdon["new_khachhangdoanhnghiep"]).Id, tienchichumia, 0, Guid.Empty, ((EntityReference)lenhdon["new_hopdongdautumia"]).Id,
                //         ((EntityReference)lenhdon["new_thuacanhtac"]).Id, (DateTime)lenhdon["new_thoigiancanra"], ref thugocchumia, ref thulaichumia);
                tienchichumia = tienchichumia - thugocchumia - thulaichumia;

                if (CS.Contains("new_dongiamiacobantairuong"))
                {
                    //tính tạm giữ vận chuyển
                    if (lenhdon.Contains("new_hopdongvanchuyen") && lenhdon.Contains("hdvc.new_giatritamgiu"))
                    {
                        decimal tamgiutoida = 0;
                        if (lenhdon.Contains("hdvc.new_tamgiutoida"))
                            tamgiutoida = ((Money)((AliasedValue)lenhdon["hdvc.new_tamgiutoida"]).Value).Value;

                        decimal datamgiu = 0;
                        if (TamgiutheoHDVC.ContainsKey(((EntityReference)lenhdon["new_hopdongvanchuyen"]).Id))
                            datamgiu = TamgiutheoHDVC[((EntityReference)lenhdon["new_hopdongvanchuyen"]).Id];
                        else
                            TamgiutheoHDVC.Add(((EntityReference)lenhdon["new_hopdongvanchuyen"]).Id, (decimal)0);

                        //tiền vạn chuyển + hỗ trợ vận chuyển
                        decimal tienvanchuyentmp = tienvanchuyen;

                        switch (((OptionSetValue)((AliasedValue)lenhdon["hdvc.new_donvitinh"]).Value).Value)
                        {
                            case 100000000: // Theo chuyến
                                if (((OptionSetValue)((AliasedValue)lenhdon["hdvc.new_cachtinhtamgiu"]).Value).Value == 100000000) //Tính theo số tiền
                                {
                                    decimal tmptamgiu1 = (decimal)((AliasedValue)lenhdon["hdvc.new_giatritamgiu"]).Value;
                                    if (tamgiutoida > 0)
                                    {
                                        tamgiuvanchuyen = (tienvanchuyentmp <= tmptamgiu1 ? tienvanchuyentmp : tmptamgiu1);
                                        tamgiuvanchuyen = ((tamgiutoida - datamgiu) <= tmptamgiu1 ? (tamgiutoida - datamgiu) : tamgiuvanchuyen);
                                    }
                                    else
                                    {
                                        tamgiuvanchuyen = tmptamgiu1;
                                    }
                                    TamgiutheoHDVC[((EntityReference)lenhdon["new_hopdongvanchuyen"]).Id] += tamgiuvanchuyen;
                                }
                                else //theo tỷ lệ %
                                {
                                    decimal tmptamgiu2 = (decimal)((AliasedValue)lenhdon["hdvc.new_giatritamgiu"]).Value / 100 * tienvanchuyentmp;
                                    if (tamgiutoida > 0)
                                    {
                                        tamgiuvanchuyen = (tienvanchuyentmp <= tmptamgiu2 ? tienvanchuyentmp : tmptamgiu2);
                                        tamgiuvanchuyen = ((tamgiutoida - datamgiu) <= tmptamgiu2 ? (tamgiutoida - datamgiu) : tamgiuvanchuyen);
                                    }
                                    else
                                    {
                                        tamgiuvanchuyen = tmptamgiu2;
                                    }
                                    TamgiutheoHDVC[((EntityReference)lenhdon["new_hopdongvanchuyen"]).Id] += tamgiuvanchuyen;
                                }
                                break;
                            default: // Sản lượng (tấn)
                                decimal tmptamgiu = (decimal)((AliasedValue)lenhdon["hdvc.new_giatritamgiu"]).Value * klthanhtoan;
                                if (tamgiutoida > 0)
                                {
                                    tamgiuvanchuyen = (tienvanchuyentmp <= tmptamgiu ? tienvanchuyentmp : tmptamgiu);
                                    tamgiuvanchuyen = ((tamgiutoida - datamgiu) <= tmptamgiu ? (tamgiutoida - datamgiu) : tamgiuvanchuyen);
                                }
                                else tamgiuvanchuyen = tmptamgiu;
                                TamgiutheoHDVC[((EntityReference)lenhdon["new_hopdongvanchuyen"]).Id] += tamgiuvanchuyen;

                                break;
                        }
                    }

                    if (lenhdon.Contains("new_hopdongvanchuyen"))
                    {
                        //Tính nợ vận chuyển
                        tienchivanchuyen = tienvanchuyen - tamgiuvanchuyen;

                        decimal thugocvanchuyen = 0;
                        decimal thulaivanchuyen = 0;
                        Guid PDNThuNoVanChuyen = Guid.Empty;
                        //if (lenhdon.Contains("new_doitacvanchuyen"))
                        //    PDNThuNoVanChuyen = Noservices.Run(VuDauTu.Id, "contact", ((EntityReference)lenhdon["new_doitacvanchuyen"]).Id, tienchivanchuyen * (lenhdon.Contains("hdvc.new_tylethuhoi") ? (decimal)((AliasedValue)lenhdon["hdvc.new_tylethuhoi"]).Value : (decimal)100) / 100, 1,
                        //          Guid.Empty, Guid.Empty, Guid.Empty, (DateTime)lenhdon["new_thoigiancanra"], ref thugocvanchuyen, ref thulaivanchuyen);
                        //else
                        //    PDNThuNoVanChuyen = Noservices.Run(VuDauTu.Id, "account", ((EntityReference)lenhdon["new_doitacvanchuyenkhdn"]).Id, tienchivanchuyen * (lenhdon.Contains("hdvc.new_tylethuhoi") ? (decimal)((AliasedValue)lenhdon["hdvc.new_tylethuhoi"]).Value : (decimal)100) / 100, 1,
                        //          Guid.Empty, Guid.Empty, Guid.Empty, (DateTime)lenhdon["new_thoigiancanra"], ref thugocvanchuyen, ref thulaivanchuyen);
                        tienchivanchuyen = tienchivanchuyen - thugocvanchuyen - thulaivanchuyen;

                        if (PDNThuNoVanChuyen != Guid.Empty)
                        {
                            tmp["new_pdnthuno_vanchuyen"] = new EntityReference("new_phieudenghithuno", PDNThuNoVanChuyen);
                            tmp["new_tienlai_vanchuyen"] = new Money(thulaivanchuyen);
                            tmp["new_trugoc_vanchuyen"] = new Money(thugocvanchuyen);
                        }
                    }
                    else
                    {
                        tienchichumia += tienvanchuyen;
                    }
                }
                else
                {
                    tienchichumia += tienvanchuyen;
                }

                decimal chiphimuamia = tienmia + tienthuongccs + (nguontienthuongtapchat == 100000000 ? (decimal)0 : tienthuongtapchat) + tienvanchuyen - tienphattapchat;

                //Input data
                tmp["new_giathuhoach"] = new Money(giacongdon);
                tmp["new_giavanchuyen"] = new Money(giavanchuyen);
                tmp["new_khoiluongthanhtoan"] = klthanhtoan;
                tmp["new_tienxe"] = new Money(tienvanchuyen);
                tmp["new_tiencongdon"] = new Money(tiencongdon);

                tmp["new_tienthuongccs"] = new Money(tienthuongccs);
                tmp["new_tienthuongtapchat"] = new Money(tienthuongtapchat);
                tmp["new_tienphattapchat"] = new Money(tienphattapchat);

                tmp["new_hotromuamia"] = new Money(klthanhtoan * dongiahotrogiamia);
                tmp["new_hotrothuhoach"] = new Money(klthanhtoan * dongiahotrocongdon);

                tmp["new_tamgiunongdan"] = new Money(tamgiuchumia);
                tmp["new_tamgiuthuhoach"] = new Money(tamgiucongdon);
                tmp["new_tamgiuvanchuyen"] = new Money(tamgiuvanchuyen);

                tmp["new_datinhtienmia"] = true;
                tmp["new_datinhtienkhuyenkhich"] = false;

                tmp["new_chiphinhamaymuamia"] = new Money(chiphimuamia);

                tmp["new_tienchidaucong"] = new Money(tienchicongdon);
                tmp["new_tienchivanchuyen"] = new Money(tienchivanchuyen);
                tmp["new_tienchichumia"] = new Money(tienchichumia);

                if (PDNThuNoChuMia != Guid.Empty)
                {
                    tmp["new_pdnthuno"] = new EntityReference("new_phieudenghithuno", PDNThuNoChuMia);
                    tmp["new_tienlai"] = new Money(thulaichumia);
                    tmp["new_trugoc"] = new Money(thugocchumia);
                }
                tmp["new_tapchatthanhtoan"] = tapchatthanhtoan;
                tmp["new_ccsthanhtoan"] = ccsThanhtoan;
                tmp["new_khoiluongthanhtoan"] = klthanhtoan;
            }

            return service.Create(tmp);
        }

        bool CheckChinhSachHDMia(Entity lenhdon, Entity CS)
        {
            if (CS.Contains("new_vutrong_vl"))
                if (!CS["new_vutrong_vl"].ToString().Contains(((OptionSetValue)((AliasedValue)lenhdon["b.new_vutrong"]).Value).Value.ToString()))
                    return false;
            if (CS.Contains("new_nhomdat_vl"))
                if (!CS["new_nhomdat_vl"].ToString().Contains(((OptionSetValue)((AliasedValue)lenhdon["b.new_nhomdat"]).Value).Value.ToString()))
                    return false;
            if (CS.Contains("new_loaigocmia_vl"))
                if (!CS["new_loaigocmia_vl"].ToString().Contains(((OptionSetValue)((AliasedValue)lenhdon["b.new_loaigocmia"]).Value).Value.ToString()))
                    return false;
            if (CS.Contains("new_luugoc") && lenhdon.Contains("b.new_luugoc"))
                if (!CS["new_luugoc"].ToString().Contains(((OptionSetValue)((AliasedValue)lenhdon["b.new_luugoc"]).Value).Value.ToString()))
                    return false;
            if (CS.Contains("new_nhomgiongmia_vl"))
                if (!CS["new_nhomgiongmia_vl"].ToString().Contains(DSGiong[((EntityReference)((AliasedValue)lenhdon["b.new_giongtrongthucte"]).Value).Id].ToString()))
                    return false;
            if (CS.Contains("new_tinhtrangmia_vl"))
                if (!CS["new_tinhtrangmia_vl"].ToString().Contains((bool)lenhdon["new_miachay"] ? "100000001" : "100000000"))
                    return false;
            if (CS.Contains("new_miachaycoy"))
                if ((bool)CS["new_miachaycoy"] != (bool)lenhdon["new_miachaycoy"])
                    return false;
            if (CS.Contains("new_loaimiachay_vl"))
                if (!CS["new_loaimiachay_vl"].ToString().Contains(((OptionSetValue)lenhdon["new_loaimiachay"]).Value.ToString()))
                    return false;
            if (CS.Contains("new_loaisohuudat_vl"))
                if (!CS["new_loaisohuudat_vl"].ToString().Contains(((OptionSetValue)((AliasedValue)lenhdon["b.new_loaisohuudat"]).Value).Value.ToString()))
                    return false;
            if (CS.Contains("new_miadonga"))
                if ((bool)CS["new_miadonga"] != (bool)lenhdon["new_miadonga"])
                    return false;
            if (CS.Contains("new_mucdichsanxuatmia_vl"))
                if (!CS["new_mucdichsanxuatmia_vl"].ToString().Contains(((OptionSetValue)((AliasedValue)lenhdon["b.new_mucdichsanxuatmia"]).Value).Value.ToString()))
                    return false;
            if (CS.Contains("new_phuongphapthuhoach_vl"))
                if (!CS["new_phuongphapthuhoach_vl"].ToString().Contains((bool)lenhdon["new_thuhoachmay"] ? "100000001" : "100000000"))
                    return false;

            //Vung dia ly
            if (CS.Contains("dspath"))
                if (lenhdon.Contains("d.new_path"))
                {
                    string path = ((AliasedValue)lenhdon["d.new_path"]).Value.ToString();
                    if (!((IEnumerable<string>)CS["dspath"]).Any(o => path.Contains(o)))
                        return false;
                }
                else return false;

            //get Giống mía dsgiong
            if (CS.Contains("dsgiong"))
                if (lenhdon.Contains("b.new_giongtrongthucte"))
                {
                    Guid giong = ((EntityReference)((AliasedValue)lenhdon["b.new_giongtrongthucte"]).Value).Id;
                    if (!((IEnumerable<Guid>)CS["dsgiong"]).Any(o => giong == o))
                        return false;
                }
                else return false;

            //get Nhóm khách hàng dsnhomkh
            if (CS.Contains("dsnhomkh"))
                if (lenhdon.Contains("e.new_nhomkhachhang") || lenhdon.Contains("f.new_nhomkhachhang"))
                {
                    Guid nhomKH = (lenhdon.Contains("e.new_nhomkhachhang") ? ((EntityReference)((AliasedValue)lenhdon["e.new_nhomkhachhang"]).Value).Id : ((EntityReference)((AliasedValue)lenhdon["f.new_nhomkhachhang"]).Value).Id);
                    if (!((IEnumerable<Guid>)CS["dsnhomkh"]).Any(o => nhomKH == o))
                        return false;
                }
                else return false;

            //get Nhóm năng suất  dsnhomns
            if (CS.Contains("dsnhomns"))
                if (lenhdon.Contains("e.new_nangsuatbinhquan") || lenhdon.Contains("f.new_nangsuatbinhquan"))
                {
                    decimal nangsuat = (lenhdon.Contains("e.new_nangsuatbinhquan") ? (decimal)((AliasedValue)lenhdon["e.new_nangsuatbinhquan"]).Value : (decimal)((AliasedValue)lenhdon["f.new_nangsuatbinhquan"]).Value);
                    Entity nhomNS = NhomNS.Entities.First(o => (decimal)o["new_nangsuattu"] <= nangsuat && (decimal)o["new_nangsuatden"] > nangsuat);
                    if (nhomNS == null)
                        return false;
                    if (!((IEnumerable<Guid>)CS["dsnhomns"]).Any(o => nhomNS.Id == o))
                        return false;
                }
                else return false;

            //get Nhóm cự ly dsnhomculy 
            if (CS.Contains("dsnhomculy"))
                if (lenhdon.Contains("b.new_nhomculy"))
                {
                    Guid culy = ((EntityReference)((AliasedValue)lenhdon["b.new_nhomculy"]).Value).Id;
                    if (!((IEnumerable<Guid>)CS["dsnhomculy"]).Any(o => culy == o))
                        return false;
                }
                else return false;

            //get Mô hình KN dsmhkn
            if (CS.Contains("dsmhkn"))
                if (lenhdon.Contains("b.new_thamgiamohinhkhuyennong"))
                {
                    Guid mhkn = ((EntityReference)((AliasedValue)lenhdon["b.new_thamgiamohinhkhuyennong"]).Value).Id;
                    if (!((IEnumerable<Guid>)CS["dsmhkn"]).Any(o => mhkn == o))
                        return false;
                }
                else return false;

            return true;
        }

        bool CheckChinhSachHDMuaNgoai(Entity lenhdon, Entity CS)
        {
            if (CS.Contains("new_tinhtrangmia_vl"))
                if (!CS["new_tinhtrangmia_vl"].ToString().Contains((bool)lenhdon["new_miachay"] ? "100000001" : "100000000"))
                    return false;
            if (CS.Contains("new_miadonga"))
                if ((bool)CS["new_miadonga"] != (bool)lenhdon["new_miadonga"])
                    return false;
            if (CS.Contains("new_phuongphapthuhoach_vl"))
                if (!CS["new_phuongphapthuhoach_vl"].ToString().Contains((bool)lenhdon["new_thuhoachmay"] ? "100000001" : "100000000"))
                    return false;

            //Vung dia ly
            if (CS.Contains("dsvung"))
                if (lenhdon.Contains("g.new_vungdialy"))
                {
                    Guid vung = ((EntityReference)((AliasedValue)lenhdon["g.new_vungdialy"]).Value).Id;
                    if (!((IEnumerable<Guid>)CS["dsvung"]).Any(o => vung == o))
                        return false;
                }
                else return false;

            //get Nhóm khách hàng dsnhomkh
            if (CS.Contains("dsnhomkh"))
                if (lenhdon.Contains("e.new_nhomkhachhang") || lenhdon.Contains("f.new_nhomkhachhang"))
                {
                    Guid nhomKH = (lenhdon.Contains("e.new_nhomkhachhang") ? ((EntityReference)((AliasedValue)lenhdon["e.new_nhomkhachhang"]).Value).Id : ((EntityReference)((AliasedValue)lenhdon["f.new_nhomkhachhang"]).Value).Id);
                    if (!((IEnumerable<Guid>)CS["dsnhomkh"]).Any(o => nhomKH == o))
                        return false;
                }
                else return false;

            //get Nhóm cự ly dsnhomculy 
            if (CS.Contains("dsnhomculy"))
                if (lenhdon.Contains("g.new_nhomculy"))
                {
                    Guid culy = ((EntityReference)((AliasedValue)lenhdon["g.new_nhomculy"]).Value).Id;
                    if (!((IEnumerable<Guid>)CS["dsnhomculy"]).Any(o => culy == o))
                        return false;
                }
                else return false;

            return true;
        }

        EntityCollection GetNhomNangSuat(Guid vuDT)
        {
            QueryExpression qe = new QueryExpression("new_nhomnangsuat");
            qe.ColumnSet = new ColumnSet(new string[] { "new_nangsuattu", "new_nangsuatden" });
            qe.Criteria.Conditions.Add(new ConditionExpression("new_vudautu", ConditionOperator.Equal, vuDT));
            return service.RetrieveMultiple(qe);
        }

        public string CreateXml(string xml, string cookie, int page, int count)
        {
            StringReader stringReader = new StringReader(xml);
            XmlTextReader reader = new XmlTextReader(stringReader);

            // Load document
            XmlDocument doc = new XmlDocument();
            doc.Load(reader);

            return CreateXml(doc, cookie, page, count);
        }

        public string CreateXml(XmlDocument doc, string cookie, int page, int count)
        {
            XmlAttributeCollection attrs = doc.DocumentElement.Attributes;

            if (cookie != null)
            {
                XmlAttribute pagingAttr = doc.CreateAttribute("paging-cookie");
                pagingAttr.Value = cookie;
                attrs.Append(pagingAttr);
            }

            XmlAttribute pageAttr = doc.CreateAttribute("page");
            pageAttr.Value = System.Convert.ToString(page);
            attrs.Append(pageAttr);

            XmlAttribute countAttr = doc.CreateAttribute("count");
            countAttr.Value = System.Convert.ToString(count);
            attrs.Append(countAttr);

            StringBuilder sb = new StringBuilder(1024);
            StringWriter stringWriter = new StringWriter(sb);

            XmlTextWriter writer = new XmlTextWriter(stringWriter);
            doc.WriteTo(writer);
            writer.Close();

            return sb.ToString();
        }

        public Boolean Evaluate(string expression)
        {
            System.Data.DataTable table = new System.Data.DataTable();
            table.Columns.Add("", typeof(Boolean));
            table.Columns[0].Expression = expression;

            System.Data.DataRow r = table.NewRow();
            table.Rows.Add(r);
            Boolean result = (Boolean)r[0];
            return result;
        }

        public void calculateSumTamGiu(Guid vuDT)
        {
            string query = string.Format(@"<fetch distinct='false' mapping='logical' aggregate='true'>
                                  <entity name='new_phieutinhtienmia'>
                                    <attribute name='new_tamgiunongdan' alias='sum_nongdan' aggregate='sum'/>
                                    <attribute name='new_thuacanhtac'  groupby='true' alias='new_thuadatcanhtacid' />
                                    <filter type = 'and' >
                                      <condition attribute='new_dachilaitamgiuchumia' operator='eq' value='0' />
                                      <condition attribute='new_vudautu' operator='eq' value='{0}' />
                                      <condition attribute='new_bangke' operator='ne' value='{1}' />
                                    </filter>
                                    <link-entity name = 'new_bangketienmia' from='new_bangketienmiaid' to='new_bangke' link-type='inner'>
                                        <filter type = 'and' >
                                          <condition attribute='statuscode' operator='eq' value='100000000' /> 
                                        </filter>
                                    </link-entity>
                                  </entity>
                                </fetch>", vuDT.ToString(), target.Id.ToString());
            FetchExpression qe = new FetchExpression(query);
            foreach (Entity a in service.RetrieveMultiple(qe).Entities)
                TamgiutheoThua.Add(((EntityReference)((AliasedValue)a["new_thuadatcanhtacid"]).Value).Id, (decimal)((AliasedValue)a["sum_nongdan"]).Value);
        }

        public void calculateSumTamGiuThuHoach(Guid vuDT)
        {
            string query = string.Format(@"<fetch distinct='false' mapping='logical' aggregate='true'>
                                  <entity name='new_phieutinhtienmia'>
                                    <attribute name='new_tamgiuthuhoach' alias='sum_thuhoach' aggregate='sum'/>
                                    <attribute name='new_hopdongthuhoach'  groupby='true' alias='new_hopdongthuhoachid' />
                                    <filter type = 'and' >
                                      <condition attribute='new_dachilaitamgiuthuhoach' operator='eq' value='0' />
                                      <condition attribute='new_vudautu' operator='eq' value='{0}' />
                                      <condition attribute='new_bangke' operator='ne' value='{1}' />
                                    </filter>
                                    <link-entity name = 'new_bangketienmia' from='new_bangketienmiaid' to='new_bangke' link-type='inner'>
                                        <filter type = 'and' >
                                         <condition attribute='statuscode' operator='eq' value='100000000' /> 
                                        </filter>
                                    </link-entity>
                                  </entity>
                                </fetch>", vuDT.ToString(), target.Id.ToString());
            FetchExpression qe = new FetchExpression(query);
            foreach (Entity a in service.RetrieveMultiple(qe).Entities)
                TamgiutheoHDTH.Add(((EntityReference)((AliasedValue)a["new_hopdongthuhoachid"]).Value).Id, (decimal)((AliasedValue)a["sum_thuhoach"]).Value);
        }

        public void calculateSumTamGiuVanChuyen(Guid vuDT)
        {
            string query = string.Format(@"<fetch distinct='false' mapping='logical' aggregate='true'>
                                  <entity name='new_phieutinhtienmia'>
                                    <attribute name='new_tamgiuvanchuyen' alias='sum_vanchuyen' aggregate='sum'/>
                                    <attribute name='new_hopdongvanchuyen'  groupby='true' alias='new_hopdongvanchuyenid' />
                                    <filter type = 'and' >
                                      <condition attribute='new_dachilaitamgiuvanchuyen' operator='eq' value='0' />
                                      <condition attribute='new_vudautu' operator='eq' value='{0}' />
                                      <condition attribute='new_bangke' operator='ne' value='{1}' />
                                    </filter>
                                    <link-entity name = 'new_bangketienmia' from='new_bangketienmiaid' to='new_bangke' link-type='inner'>
                                         <filter type = 'and' >
                                               <condition attribute='statuscode' operator='eq' value='100000000' /> 
                                          </filter>
                                    </link-entity>
                                  </entity>
                                </fetch>", vuDT.ToString(), target.Id.ToString());
            FetchExpression qe = new FetchExpression(query);
            foreach (Entity a in service.RetrieveMultiple(qe).Entities)
                TamgiutheoHDVC.Add(((EntityReference)((AliasedValue)a["new_hopdongvanchuyenid"]).Value).Id, (decimal)((AliasedValue)a["sum_vanchuyen"]).Value);
        }

        public void loadBBThoaThuanCongDon(Guid vuDT)
        {
            string query = string.Format(@"<fetch version = '1.0' output-format='xml-platform' mapping='logical' distinct='true'>
                              <entity name = 'new_chitietbbthoathuancongdon' >
                                <attribute name='new_thuadatcanhtac' />
                                <attribute name='new_congdonchatvabocmia' />
                                <attribute name='new_giacongmiachay' />
                                <attribute name='new_trungchuyen' />
                                <attribute name='new_giatongcong' />
                                <link-entity name = 'new_bienbanthoathuancongdon' from='new_bienbanthoathuancongdonid' to='new_bbthoathuancongdon' link-type='inner' alias='b' >
                                  <attribute name='new_daucongkh' />
                                  <attribute name='new_daucongkhdn' />
                                  <attribute name='new_daucong2kh' />
                                  <attribute name='new_daucong2khdn' />
                                  <filter type = 'and' >
                                    <condition attribute='statuscode' operator='eq' value='100000000' />
                                    <condition attribute='new_vudautu' operator='eq' value='{0}' />
                                  </filter>
                                </link-entity>
                              </entity>
                            </fetch>", vuDT.ToString());
            FetchExpression qe = new FetchExpression(query);

            DSTTCongDon = service.RetrieveMultiple(qe);
        }

        public void loadBangGiaVanChuyen(Guid vuDT)
        {
            QueryExpression qe = new QueryExpression("new_banggiavanchuyen");
            qe.ColumnSet = new ColumnSet("new_banggiavanchuyenid");

            qe.Orders.Add(new OrderExpression("new_thoidiemapdung", OrderType.Descending));
            LinkEntity l1 = new LinkEntity("new_banggiavanchuyen", "new_hesotanggiamtheovung", "new_banggiavanchuyenid", "new_banggiavanchuyen", JoinOperator.Inner);
            l1.Columns = new ColumnSet(new string[] { "new_ngayapdung", "new_vungdialy", "new_culy", "new_giacuoc" });
            l1.EntityAlias = "a";
            qe.Criteria.AddCondition(new ConditionExpression("new_vudautu", ConditionOperator.Equal, vuDT));
            qe.Criteria.AddCondition(new ConditionExpression("statuscode", ConditionOperator.Equal, 100000000));
            qe.LinkEntities.Add(l1);

            BangGiaVanChuyen = service.RetrieveMultiple(qe);
        }

        public void loadGiong()
        {
            QueryExpression qe = new QueryExpression("new_giongmia");
            qe.ColumnSet = new ColumnSet("new_nhomgiong");
            foreach (Entity a in service.RetrieveMultiple(qe).Entities)
                DSGiong.Add(a.Id, ((OptionSetValue)a["new_nhomgiong"]).Value);
        }

        public void loadBBThuHoachSom(Guid vuDT)
        {
            QueryExpression qe = new QueryExpression("new_chitietbbthuhoachsom");
            qe.ColumnSet = new ColumnSet(new string[] { "new_chitiethddtmia", "new_tamgiu", "new_baohiemccs", "new_dientichthsom", "new_ngayhethan" });
            qe.LinkEntities.Add(new LinkEntity("new_chitietbbthuhoachsom", "new_bienbanthuhoachsom", "new_bienbanthuhoachsom", "new_bienbanthuhoachsomid", JoinOperator.Inner));
            qe.LinkEntities[0].LinkCriteria.Conditions.Add(new ConditionExpression("new_vudautu", ConditionOperator.Equal, vuDT));
            qe.LinkEntities[0].LinkCriteria.Conditions.Add(new ConditionExpression("statuscode", ConditionOperator.Equal, 100000000));

            foreach (Entity a in service.RetrieveMultiple(qe).Entities)
                DSBBThuHoachSom.Add(((EntityReference)a["new_chitiethddtmia"]).Id, a);
        }
    }
}
