using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Web.Script.Serialization;
using System.Net;
using Microsoft.Xrm.Sdk.Messages;
using System.Text;
using System.Linq;
using System.IO;
using System.Xml;

namespace Action_GetChinhSachCanDo
{
    public class Action_GetChinhSachCanDo : IPlugin
    {
        IOrganizationService service = null;
        IOrganizationServiceFactory factory = null;
        EntityCollection NhomNS = new EntityCollection();

        void IPlugin.Execute(IServiceProvider serviceProvider)
        {
            IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
            service = factory.CreateOrganizationService(context.UserId);

            try
            {
                Guid LenhdonId = Guid.Parse(context.InputParameters["new_lenhdon"].ToString());
                Guid VuDauTuId = Guid.Parse(context.InputParameters["new_vudautu"].ToString());
                decimal tapchatthanhtoan = decimal.Parse(context.InputParameters["new_tapchatthucte"].ToString());
                decimal ccsThanhtoan = decimal.Parse(context.InputParameters["new_ccsthucte"].ToString());
                decimal tlthucte = decimal.Parse(context.InputParameters["new_trongluongthucte"].ToString());

                Entity LenhDon = GetLenhDon(LenhdonId);
                if (LenhDon == null) throw new Exception("Không tìm thấy lệnh đốn !");

                EntityCollection CSTM = GetChinhSachThuMua(VuDauTuId);
                NhomNS = GetNhomNangSuat(VuDauTuId);
                Entity CS = null;

                if (LenhDon.Contains("new_hopdongdautumia"))
                    //Check CS in HD dau tu mia
                    foreach (Entity a in GetAllHDMiainCS(VuDauTuId, ((EntityReference)LenhDon["new_hopdongdautumia"]).Id).Entities)
                    {
                        if (DateTime.Now >= (DateTime)a["new_thoidiemapdung"])
                        {
                            CS = CSTM.Entities.Where(o => o.Id == a.Id).ToArray()[0];
                            break;
                        }
                    }
                else
                    //Check CS in HD mia ngoai
                    foreach (Entity a in GetAllHDMuaNgoaiinCS(VuDauTuId, ((EntityReference)LenhDon["new_hopdongmuabanmiangoai"]).Id).Entities)
                    {
                        if (DateTime.Now >= (DateTime)a["new_thoidiemapdung"])
                        {
                            CS = CSTM.Entities.Where(o => o.Id == a.Id).ToArray()[0];
                            break;
                        }
                    }

                if (CS == null) //Chưa có chính sách
                {
                    if (LenhDon.Contains("new_hopdongdautumia"))
                        foreach (Entity cs in CSTM.Entities.Where(o => ((OptionSetValue)o["new_hoatdongapdung"]).Value == 100000000).OrderByDescending(o => o["new_thoidiemapdung"]))
                        {
                            if (DateTime.Now >= (DateTime)cs["new_thoidiemapdung"])
                            {
                                if (CheckChinhSachHDMia(LenhDon, cs))
                                {
                                    CS = cs;
                                    break;
                                }
                            }
                        }
                    else
                        foreach (Entity cs in CSTM.Entities.Where(o => ((OptionSetValue)o["new_hoatdongapdung"]).Value == 100000003).OrderByDescending(o => o["new_thoidiemapdung"]))
                        {
                            if (DateTime.Now >= (DateTime)cs["new_thoidiemapdung"])
                            {
                                if (CheckChinhSachHDMuaNgoai(LenhDon, cs))
                                {
                                    CS = cs;
                                    break;
                                }
                            }
                        }
                }

                if (CS == null)
                    throw new Exception("Không tìm thấy chính sách !");
                else
                {
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
                                }
                                break;
                            }
                        }
                    #endregion

                    //Tính ccs thanh toán ccsbaoccsbao_ths- CCS bao
                    if (ccsThanhtoan > 0 && LenhDon.Contains("h.new_chitietbbthuhoachsomid") && (bool)((AliasedValue)LenhDon["h.new_baohiemccs"]).Value
                        && (LenhDon.Contains("h.new_ngayhethan") && (DateTime)LenhDon["new_ngaycap"] <= (DateTime)((AliasedValue)LenhDon["h.new_ngayhethan"]).Value)
                        && !(bool)LenhDon["new_miadutuoi"])
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

                    #region update lenh don
                    Entity ld = new Entity("new_lenhdon");
                    ld.Id = LenhdonId;
                    ld["new_tapchatthanhtoan"] = tapchatthanhtoan;
                    ld["new_chinhsachthumua"] = CS.ToEntityReference();
                    if (ccsThanhtoan > 0)
                        ld["new_ccsthanhtoan"] = ccsThanhtoan;
                    if (tlthucte > 0)
                        ld["new_khoiluongthanhtoan"] = tlthucte - (tlthucte * tapchatthanhtoan / 100);
                    service.Update(ld);
                    #endregion
                }

                context.OutputParameters["Return"] = string.Format("tc:{0};ccs:{1}", tapchatthanhtoan, ccsThanhtoan);
            }
            catch (Exception ex)
            {
                context.OutputParameters["Return"] = "Error : " + ex.Message;
            }
        }

        Entity GetLenhDon(Guid LenhdonId)
        {
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
<attribute name='new_chitiethopdongmuabanmiangoai' />
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
<link-entity name = 'new_giongmia' from='new_giongmiaid' to='new_giongtrongthucte' link-type='outer' alias='c'>
<attribute name='new_nhomgiong' />
</link-entity>
<link-entity name = 'new_thuadat' from='new_thuadatid' to='new_thuadat' link-type='outer' alias='i'>
<attribute name='new_vungdialy' />
<link-entity name = 'new_diachi' from='new_diachiid' to='new_diachi' link-type='outer' alias='d'>
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
<link-entity name = 'new_chitietbbthuhoachsom' from='new_chitiethddtmia' to='new_thuadatcanhtacid' link-type='outer' alias='h'>
<attribute name='new_chitietbbthuhoachsomid' />
<attribute name='new_baohiemccs' />
<attribute name='new_tamgiu' />
<attribute name='new_dientichthsom' />
<attribute name='new_ngayhethan' />
</link-entity>
</link-entity>
<link-entity name = 'new_chitiethopdongmuabanmiangoai' from='new_chitiethopdongmuabanmiangoaiid' to='new_chitiethopdongmuabanmiangoai' link-type='outer' alias='g' >
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
<condition attribute='new_lenhdonid' operator='eq' value='{0}' />
</filter>
</entity>
</fetch>", LenhdonId.ToString());
            FetchExpression ft = new FetchExpression(query);
            EntityCollection result = service.RetrieveMultiple(ft);
            if (result.Entities.Count > 0)
                return result[0];
            else return null;
        }

        EntityCollection GetAllHDMiainCS(Guid VuDauTu, Guid HDMia)
        {
            string query = " <fetch version = '1.0' output-format='xml-platform' mapping='logical' distinct='true'> " +
             " <entity name = 'new_chinhsachthumua' > " +
            " <attribute name='new_chinhsachthumuaid' /> " +
            " <attribute name='new_thoidiemapdung' /> " +
            " <order attribute='new_thoidiemapdung' descending='true' />" +
            " <link-entity name = 'new_new_chinhsachthumua_new_hopdongdautumia' from='new_chinhsachthumuaid' to='new_chinhsachthumuaid' link-type='inner' > " +
            " <link-entity name = 'new_hopdongdautumia' from='new_hopdongdautumiaid' to='new_hopdongdautumiaid' link-type='inner' alias='b'> " +
             " <attribute name = 'new_hopdongdautumiaid' /> " +
            " <filter type = 'and' > " +
            " <condition attribute='new_hopdongdautumiaid' operator='eq' value='" + HDMia.ToString() + "' /> " +
            " </filter> " +
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

        EntityCollection GetAllHDMuaNgoaiinCS(Guid VuDauTu, Guid HDMiaNgoai)
        {
            string query = " <fetch version = '1.0' output-format='xml-platform' mapping='logical' distinct='true'> " +
            " <entity name = 'new_chinhsachthumua' > " +
            " <attribute name='new_chinhsachthumuaid' /> " +
            " <attribute name='new_thoidiemapdung' /> " +
            " <order attribute='new_thoidiemapdung' descending='true' />" +
            " <link-entity name = 'new_new_chinhsachthumua_new_hopdongmuabanmi' from='new_chinhsachthumuaid' to='new_chinhsachthumuaid' link-type='inner' > " +
            " <link-entity name = 'new_hopdongmuabanmiangoai' from='new_hopdongmuabanmiangoaiid' to='new_hopdongmuabanmiangoaiid' link-type='inner' > " +
            " <attribute name = 'new_hopdongmuabanmiangoaiid' /> " +
            " <filter type = 'and' > " +
            " <condition attribute='new_hopdongmuabanmiangoaiid' operator='eq' value='" + HDMiaNgoai.ToString() + "' /> " +
            " </filter> " +
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
                EntityCollection ccsbao_ths = RetrieveMulti("new_chinhsachthumua_ccsbao_thuhoachsom", "new_chinhsachthumua", a.Id, "new_tu");
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

        EntityCollection RetrieveMulti(string entity, string cond, object value, string order)
        {
            QueryExpression qe = new QueryExpression(entity);
            qe.ColumnSet = new ColumnSet(true);
            qe.Criteria.Conditions.Add(new ConditionExpression(cond, ConditionOperator.Equal, value));
            qe.Criteria.Conditions.Add(new ConditionExpression("statecode", ConditionOperator.Equal, 0));
            qe.Orders.Add(new OrderExpression(order, OrderType.Ascending));

            return service.RetrieveMultiple(qe);
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
                if (!CS["new_nhomgiongmia_vl"].ToString().Contains(((OptionSetValue)((AliasedValue)lenhdon["c.new_nhomgiong"]).Value).ToString()))
                    return false;
            if (CS.Contains("new_tinhtrangmia_vl"))
                if (!CS["new_tinhtrangmia_vl"].ToString().Contains((bool)lenhdon["new_miachay"] ? "100000001" : "100000000"))
                    return false;
            if (CS.Contains("new_miachaycoy") && (bool)CS["new_miachaycoy"])
                if ((bool)CS["new_miachaycoy"] != (bool)lenhdon["new_miachaycoy"])
                    return false;
            if (CS.Contains("new_loaimiachay_vl"))
                if (!CS["new_loaimiachay_vl"].ToString().Contains(((OptionSetValue)lenhdon["new_loaimiachay"]).Value.ToString()))
                    return false;
            if (CS.Contains("new_loaisohuudat_vl"))
                if (!CS["new_loaisohuudat_vl"].ToString().Contains(((OptionSetValue)((AliasedValue)lenhdon["b.new_loaisohuudat"]).Value).Value.ToString()))
                    return false;
            if (CS.Contains("new_miadonga") && (bool)CS["new_miadonga"])
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

            //get Nhóm năng suấtdsnhomns
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
            if (CS.Contains("new_miadonga") && (bool)CS["new_miadonga"])
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
    }
}
