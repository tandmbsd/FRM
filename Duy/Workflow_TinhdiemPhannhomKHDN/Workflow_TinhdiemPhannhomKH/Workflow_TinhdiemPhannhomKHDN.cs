using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Workflow;
using Microsoft.Xrm.Sdk.Query;
using Microsoft.Crm.Sdk.Messages;
using System.Activities;

namespace Workflow_TinhdiemPhannhomKH
{
    public class Workflow_TinhdiemPhannhomKHDN : CodeActivity
    {
        [RequiredArgument]
        [Input("InputEntity")]
        [ReferenceTarget("account")]
        public InArgument<EntityReference> inputEntity { get; set; }

        public IOrganizationService service;
        protected override void Execute(CodeActivityContext executionContext)
        {
            ITracingService tracingService = executionContext.GetExtension<ITracingService>();
            IWorkflowContext context = executionContext.GetExtension<IWorkflowContext>();
            IOrganizationServiceFactory serviceFactory = executionContext.GetExtension<IOrganizationServiceFactory>();
            service = serviceFactory.CreateOrganizationService(context.UserId);

            Guid enId = this.inputEntity.Get(executionContext).Id;
            EntityReference entrf = this.inputEntity.Get(executionContext);
            #region khach hang
            if (entrf.LogicalName == "contact") // khach hang
            {
                Entity khachhangEn = this.service.Retrieve(entrf.LogicalName, entrf.Id, new ColumnSet(new string[] { "new_thoigianhoptac", "new_nangsuatbinhquan", "new_culybinhquan", "new_nangluctaichinh_diem", "new_nangsuatduong" }));
                string khachhangEnID = khachhangEn.Id.ToString();
                decimal tongdiem = new decimal();
                decimal diemcong = new decimal();
                decimal nangsuatbinhquan = new decimal();
                decimal a1 = 0;
                decimal b = 0;
                decimal c = 0;
                decimal d = 0;
                decimal e = 0;
                decimal f = 0;
                decimal g = 0;
                decimal h = 0;
                decimal noquahan1 = 0;
                bool isNoquahan = false;
                Entity vudautuhientai = RetrieveSingleRecord(service, "new_vudautu", new ColumnSet(true), "new_danghoatdong", true);
                Entity tieuchiphannhomKH = RetrieveSingleRecord(service, "new_tieuchiphannhomkhachhang", new ColumnSet(new string[] { "new_tytrong_tght", "new_tytrong_nsbq", "new_tytrong_clbq", "new_tytrong_qmdt", "new_tytrong_nqh", "new_tytrong_nltc", "new_name" }), "new_vudautu", vudautuhientai.Id);

                // tinh diem tieu chi 
                QueryExpression qHddtmhientai = new QueryExpression("new_hopdongdautumia");
                qHddtmhientai.ColumnSet = new ColumnSet(true);
                qHddtmhientai.Criteria = new FilterExpression();
                qHddtmhientai.Criteria.AddCondition(new ConditionExpression("new_vudautu", ConditionOperator.Equal, vudautuhientai.Id));
                qHddtmhientai.Criteria.AddCondition(new ConditionExpression("new_khachhang", ConditionOperator.Equal, khachhangEn.Id));
                EntityCollection entcHddtm = service.RetrieveMultiple(qHddtmhientai);
                List<Entity> lstHDDTM = entcHddtm.Entities.ToList<Entity>();
                if (lstHDDTM.Count <= 0)
                {
                    throw new Exception("Khách hàng không có hợp đồng nào !!!");
                }
                if (!tieuchiphannhomKH.Contains("new_tytrong_tght"))
                    throw new Exception("Chưa có tỷ trọng thời gian hợp tác !!");
                decimal tytrongTGHT = tieuchiphannhomKH.GetAttributeValue<decimal>("new_tytrong_tght");

                if (!tieuchiphannhomKH.Contains("new_tytrong_nsbq"))
                    throw new Exception("Chưa có tỷ trọng năng suất bình quân đường !!");
                decimal tytrongNSBQD = tieuchiphannhomKH.GetAttributeValue<decimal>("new_tytrong_nsbq");

                if (!tieuchiphannhomKH.Contains("new_tytrong_clbq"))
                    throw new Exception("Chưa có tỷ trọng cự li bình quân !!");
                decimal tytrongCLBQ = tieuchiphannhomKH.GetAttributeValue<decimal>("new_tytrong_clbq");

                if (!tieuchiphannhomKH.Contains("new_tytrong_qmdt"))
                    throw new Exception("Chưa có tỷ trọng quy mô diện tích !!");
                decimal tytrongQMDT = tieuchiphannhomKH.GetAttributeValue<decimal>("new_tytrong_qmdt");

                if (!tieuchiphannhomKH.Contains("new_tytrong_nqh"))
                    throw new Exception("Chưa có tỷ trọng nợ quá hạn !!");
                decimal tytrongNQH = tieuchiphannhomKH.GetAttributeValue<decimal>("new_tytrong_nqh");

                if (!tieuchiphannhomKH.Contains("new_tytrong_nltc"))
                    throw new Exception("Chưa có tỷ trọng năng lực tài chính !!");
                decimal tytrongNLTC = tieuchiphannhomKH.GetAttributeValue<decimal>("new_tytrong_nltc");
                #region Thoigianhoptac
                QueryExpression q = new QueryExpression("new_cosotinhdiem");
                q.ColumnSet = new ColumnSet(true);
                q.Criteria = new FilterExpression();
                q.Criteria.AddCondition(new ConditionExpression("new_loaitieuchi", ConditionOperator.Equal, "100000000"));
                q.Criteria.AddCondition(new ConditionExpression("new_tieuchiphannhomkhachhang", ConditionOperator.Equal, tieuchiphannhomKH.Id));
                EntityCollection entc = service.RetrieveMultiple(q);
                List<Entity> lstCosotinhdiemTGHT = entc.Entities.ToList<Entity>();

                foreach (Entity TGHT in lstCosotinhdiemTGHT)
                {
                    int pheptinhtu = ((OptionSetValue)TGHT["new_pheptinhtu"]).Value;
                    int pheptinhden = ((OptionSetValue)TGHT["new_pheptinhden"]).Value;

                    if (!khachhangEn.Contains("new_thoigianhoptac"))
                    {
                        throw new Exception("Khách hàng chưa có thời gian hợp tác");
                    }
                    decimal tght = (decimal)(khachhangEn.GetAttributeValue<int>("new_thoigianhoptac"));

                    if (tinhdiem(pheptinhtu, TGHT.GetAttributeValue<decimal>("new_giatritu"), pheptinhden, TGHT.GetAttributeValue<decimal>("new_giatriden"), tght))
                    {
                        tongdiem = tongdiem + ((decimal)(TGHT.GetAttributeValue<int>("new_diem")) * tytrongTGHT / 100);
                    }
                }

                #endregion
                #region NSBQD
                QueryExpression q1 = new QueryExpression("new_cosotinhdiem");
                q1.ColumnSet = new ColumnSet(true);
                q1.Criteria = new FilterExpression();
                q1.Criteria.AddCondition(new ConditionExpression("new_loaitieuchi", ConditionOperator.Equal, "100000001"));
                q1.Criteria.AddCondition(new ConditionExpression("new_tieuchiphannhomkhachhang", ConditionOperator.Equal, tieuchiphannhomKH.Id));
                EntityCollection entc1 = service.RetrieveMultiple(q1);
                List<Entity> lstCosotinhdiemNSBQD = entc1.Entities.ToList<Entity>();

                QueryExpression qVudautu = new QueryExpression("new_vudautu");
                qVudautu.ColumnSet = new ColumnSet(true);
                qVudautu.Criteria = new FilterExpression();
                qVudautu.Criteria.AddCondition(new ConditionExpression("statecode", ConditionOperator.Equal, 0));
                EntityCollection a = service.RetrieveMultiple(qVudautu);

                List<Entity> lst = a.Entities.OrderBy(p => p.GetAttributeValue<DateTime>("new_ngaybatdau")).ToList<Entity>();
                int curr = lst.FindIndex(p => p.Id == vudautuhientai.Id);

                List<Entity> lstHDDTM3nam = new List<Entity>();
                Entity khEditNSBQ = new Entity("contact");
                if (3 <= curr)
                {
                    for (int i = 1; i <= 3; i++)
                    {
                        QueryExpression qHddtm3nam = new QueryExpression("new_hopdongdautumia");
                        qHddtmhientai.ColumnSet = new ColumnSet(true);
                        qHddtmhientai.Criteria = new FilterExpression();
                        qHddtmhientai.Criteria.AddCondition(new ConditionExpression("new_vudautu", ConditionOperator.Equal, lst[curr - i].Id));
                        qHddtmhientai.Criteria.AddCondition(new ConditionExpression("new_khachhang", ConditionOperator.Equal, khachhangEn.Id));
                        EntityCollection entcHddtm3nam = service.RetrieveMultiple(qHddtmhientai);
                        lstHDDTM3nam = entcHddtm3nam.Entities.ToList<Entity>();

                        nangsuatbinhquan += sumnangsuatbinhquan(new EntityCollection(lstHDDTM3nam));
                    }
                }
                else if (curr == 2)
                {
                    for (int i = 1; i <= 2; i++)
                    {
                        QueryExpression qHddtm3nam = new QueryExpression("new_hopdongdautumia");
                        qHddtmhientai.ColumnSet = new ColumnSet(true);
                        qHddtmhientai.Criteria = new FilterExpression();
                        qHddtmhientai.Criteria.AddCondition(new ConditionExpression("new_vudautu", ConditionOperator.Equal, lst[curr - i].Id));
                        qHddtmhientai.Criteria.AddCondition(new ConditionExpression("new_khachhang", ConditionOperator.Equal, khachhangEn.Id));
                        EntityCollection entcHddtm3nam = service.RetrieveMultiple(qHddtmhientai);
                        lstHDDTM3nam = entcHddtm3nam.Entities.ToList<Entity>();
                        nangsuatbinhquan += sumnangsuatbinhquan(new EntityCollection(lstHDDTM3nam));

                    }
                }
                else if (curr == 1)
                {
                    QueryExpression qHddtm3nam = new QueryExpression("new_hopdongdautumia");
                    qHddtmhientai.ColumnSet = new ColumnSet(true);
                    qHddtmhientai.Criteria = new FilterExpression();
                    qHddtmhientai.Criteria.AddCondition(new ConditionExpression("new_vudautu", ConditionOperator.Equal, lst[curr - 1].Id));
                    qHddtmhientai.Criteria.AddCondition(new ConditionExpression("new_khachhang", ConditionOperator.Equal, khachhangEn.Id));
                    EntityCollection entcHddtm3nam = service.RetrieveMultiple(qHddtmhientai);
                    Entity temp = entcHddtm3nam.Entities.ToList<Entity>().FirstOrDefault();

                    nangsuatbinhquan = temp.Contains("new_nangsuatduong") ? temp.GetAttributeValue<decimal>("new_nangsuatbinhquan") : 0;
                }

                khEditNSBQ = service.Retrieve(khachhangEn.LogicalName, khachhangEn.Id, new ColumnSet(new string[] { "new_nangsuatduong", "new_makhachhang" }));
                khEditNSBQ["new_nangsuatduong"] = nangsuatbinhquan;

                service.Update(khEditNSBQ);

                foreach (Entity NSBQD in lstCosotinhdiemNSBQD)
                {
                    int pheptinhtu = ((OptionSetValue)NSBQD["new_pheptinhtu"]).Value;
                    int pheptinhden = ((OptionSetValue)NSBQD["new_pheptinhden"]).Value;

                    if (!khachhangEn.Contains("new_nangsuatduong"))
                    {
                        throw new Exception("Khách hàng chưa có năng suất đường");
                    }
                    //decimal nangsuatduong = (decimal)(khachhangEn.GetAttributeValue<decimal>("new_nangsuatduong"));

                    if (tinhdiem(pheptinhtu, NSBQD.GetAttributeValue<decimal>("new_giatritu"), pheptinhden, NSBQD.GetAttributeValue<decimal>("new_giatriden"), nangsuatbinhquan))
                    {
                        tongdiem = tongdiem + (decimal)(NSBQD.GetAttributeValue<int>("new_diem")) * tytrongNSBQD / 100;
                    }
                }

                #endregion
                #region Culibinhquan
                QueryExpression q2 = new QueryExpression("new_cosotinhdiem");
                q2.ColumnSet = new ColumnSet(true);
                q2.Criteria = new FilterExpression();
                q2.Criteria.AddCondition(new ConditionExpression("new_loaitieuchi", ConditionOperator.Equal, "100000002"));
                q2.Criteria.AddCondition(new ConditionExpression("new_tieuchiphannhomkhachhang", ConditionOperator.Equal, tieuchiphannhomKH.Id));
                EntityCollection entc2 = service.RetrieveMultiple(q2);
                List<Entity> lstCosotinhdiemCLBQ = entc2.Entities.ToList<Entity>();
                foreach (Entity CLBQ in lstCosotinhdiemCLBQ)
                {
                    int pheptinhtu = ((OptionSetValue)CLBQ["new_pheptinhtu"]).Value;
                    int pheptinhden = ((OptionSetValue)CLBQ["new_pheptinhden"]).Value;

                    if (!khachhangEn.Contains("new_culybinhquan"))
                    {
                        throw new Exception("Khách hàng chưa có cự li bình quân !");
                    }
                    decimal clbq = khachhangEn.GetAttributeValue<decimal>("new_culybinhquan");

                    if (tinhdiem(pheptinhtu, CLBQ.GetAttributeValue<decimal>("new_giatritu"), pheptinhden, CLBQ.GetAttributeValue<decimal>("new_giatriden"), clbq))
                    {
                        tongdiem = tongdiem + (decimal)(CLBQ.GetAttributeValue<int>("new_diem")) * tytrongCLBQ / 100;
                    }
                }
                #endregion
                #region Quimodientich
                QueryExpression q3 = new QueryExpression("new_cosotinhdiem");
                q3.ColumnSet = new ColumnSet(true);
                q3.Criteria = new FilterExpression();
                q3.Criteria.AddCondition(new ConditionExpression("new_loaitieuchi", ConditionOperator.Equal, "100000003"));
                q3.Criteria.AddCondition(new ConditionExpression("new_tieuchiphannhomkhachhang", ConditionOperator.Equal, tieuchiphannhomKH.Id));
                EntityCollection entc3 = service.RetrieveMultiple(q3);
                List<Entity> lstCosotinhdiemQMDT = entc3.Entities.ToList<Entity>();
                decimal tongdientich = 0;

                foreach (Entity hopdongdautumia in lstHDDTM)
                {
                    List<Entity> Lstthuadatcanhtac = RetrieveMultiRecord(service, "new_thuadatcanhtac",
                        new ColumnSet(new string[] { "new_dientichthucte" }), "new_hopdongdautumia", hopdongdautumia.Id);
                    tongdientich = tongdientich + sumdientichthucte(new EntityCollection(Lstthuadatcanhtac));
                }

                foreach (Entity QMDT in lstCosotinhdiemQMDT)
                {
                    int pheptinhtu = ((OptionSetValue)QMDT["new_pheptinhtu"]).Value;
                    int pheptinhden = ((OptionSetValue)QMDT["new_pheptinhden"]).Value;

                    if (tinhdiem(pheptinhtu, QMDT.GetAttributeValue<decimal>("new_giatritu"), pheptinhden, QMDT.GetAttributeValue<decimal>("new_giatriden"), tongdientich))
                    {
                        tongdiem = tongdiem + (decimal)(QMDT.GetAttributeValue<int>("new_diem")) * tytrongQMDT / 100;
                    }
                }

                #endregion
                #region Noquahan
                QueryExpression q4 = new QueryExpression("new_cosotinhdiem");
                q4.ColumnSet = new ColumnSet(true);
                q4.Criteria = new FilterExpression();
                q4.Criteria.AddCondition(new ConditionExpression("new_loaitieuchi", ConditionOperator.Equal, "100000004"));
                q4.Criteria.AddCondition(new ConditionExpression("new_tieuchiphannhomkhachhang", ConditionOperator.Equal, tieuchiphannhomKH.Id));
                EntityCollection entc4 = service.RetrieveMultiple(q4);
                List<Entity> lstCosotinhdiemNQH = entc4.Entities.ToList<Entity>();
                decimal noquahan = 0;
                QueryExpression q5 = new QueryExpression("new_phanbodautu");
                q5.ColumnSet = new ColumnSet(true);
                q5.Criteria = new FilterExpression();
                q5.Criteria.AddCondition(new ConditionExpression("new_vudautu", ConditionOperator.Equal, vudautuhientai.Id));
                q5.Criteria.AddCondition(new ConditionExpression("new_khachhang", ConditionOperator.Equal, khachhangEn.Id));
                EntityCollection entc5 = service.RetrieveMultiple(q5);
                List<Entity> lstPhanbodautu = entc5.Entities.ToList<Entity>();

                foreach (Entity pbdt in lstPhanbodautu)
                {
                    if (pbdt.GetAttributeValue<DateTime>("new_hanthanhtoan") < DateTime.Today)
                    {
                        isNoquahan = true;
                        noquahan = noquahan + ((Money)pbdt["new_conlai"]).Value;
                    }
                }
                //throw new Exception(noquahan.ToString() + isNoquahan.ToString());
                foreach (Entity NQH in lstCosotinhdiemNQH)
                {
                    int pheptinhtu = ((OptionSetValue)NQH["new_pheptinhtu"]).Value;
                    int pheptinhden = ((OptionSetValue)NQH["new_pheptinhden"]).Value;
                    string cachtinh = ((OptionSetValue)NQH["new_cachtinh"]).Value.ToString();
                    decimal giatritu = 0;
                    decimal giatriden = 0;

                    if (cachtinh == "100000001")
                    {
                        giatritu = ((Money)NQH["new_sotientu"]).Value;
                        giatriden = ((Money)NQH["new_sotienden"]).Value;
                    }
                    else
                    {
                        giatritu = NQH.GetAttributeValue<decimal>("new_giatritu");
                        giatriden = NQH.GetAttributeValue<decimal>("new_giatriden");
                    }

                    if (tinhdiem(pheptinhtu, giatritu, pheptinhden, giatriden, noquahan))
                    {

                        tongdiem = tongdiem + (decimal)(NQH.GetAttributeValue<int>("new_diem")) * tytrongNQH / 100;
                        noquahan1 = (decimal)(NQH.GetAttributeValue<int>("new_diem")) * tytrongNQH / 100;
                    }
                }
                #endregion
                #region Nangluctaichinh
                QueryExpression q6 = new QueryExpression("new_cosotinhdiem");
                q6.ColumnSet = new ColumnSet(true);
                q6.Criteria = new FilterExpression();
                q6.Criteria.AddCondition(new ConditionExpression("new_loaitieuchi", ConditionOperator.Equal, "100000005"));
                q6.Criteria.AddCondition(new ConditionExpression("new_tieuchiphannhomkhachhang", ConditionOperator.Equal, tieuchiphannhomKH.Id));
                EntityCollection entc6 = service.RetrieveMultiple(q6);
                List<Entity> lstCosotinhdiemNLTC = entc6.Entities.ToList<Entity>();
                foreach (Entity NLTC in lstCosotinhdiemNLTC)
                {
                    int pheptinhtu = ((OptionSetValue)NLTC["new_pheptinhtu"]).Value;
                    int pheptinhden = ((OptionSetValue)NLTC["new_pheptinhden"]).Value;

                    if (!khachhangEn.Contains("new_nangluctaichinh_diem"))
                    {
                        throw new Exception("Khách hàng chưa có năng lực tài chính");
                    }

                    decimal nltc = (decimal)(khachhangEn.GetAttributeValue<int>("new_nangluctaichinh_diem"));

                    if (tinhdiem(pheptinhtu, NLTC.GetAttributeValue<decimal>("new_giatritu"), pheptinhden, NLTC.GetAttributeValue<decimal>("new_giatriden"), nltc))
                    {
                        tongdiem = tongdiem + ((decimal)(NLTC.GetAttributeValue<int>("new_diem")) * tytrongNLTC / 100);

                    }
                }

                #endregion

                //tinh diem cong tru
                List<Entity> lstCachtinhdiemcongtru = RetrieveMultiRecord(service, "new_cachtinhdiemcongtru", new ColumnSet(true), "new_tieuchiphannhomkh", tieuchiphannhomKH.Id);
                foreach (Entity ctdiemcongtru in lstCachtinhdiemcongtru)
                {
                    decimal tongdientichmiatuoi = 0;
                    decimal dientichtrongmia = 0;
                    Entity hdonly = lstHDDTM.FirstOrDefault();
                    decimal dientichtrongmiaonly = hdonly.GetAttributeValue<decimal>("new_dientichtrongmia");

                    string loaitinhdiem = ((OptionSetValue)ctdiemcongtru["new_loaitinhdiem"]).Value.ToString();
                    string phuongthuctinh = ((OptionSetValue)ctdiemcongtru["new_phuongthuctinh"]).Value.ToString();
                    #region dien tich tuoi mia huu hieu > 30% dien tich hop dong
                    if (loaitinhdiem == "100000000")//dien tich tuoi mia huu hieu > 30% dien tich hop dong 
                    {
                        foreach (Entity hopdongdautumia in lstHDDTM)
                        {
                            if (hopdongdautumia.Contains("new_dientichtrongmia"))
                            {
                                dientichtrongmia += hopdongdautumia.GetAttributeValue<decimal>("new_dientichtrongmia");
                            }
                            List<Entity> Lstthuadatcanhtac = RetrieveMultiRecord(service, "new_thuadatcanhtac",
                                new ColumnSet(new string[] { "new_dientichmiatuoi" }), "new_hopdongdautumia", hopdongdautumia.Id);
                            tongdientichmiatuoi = tongdientichmiatuoi + sumdientichmiatuoi(new EntityCollection(Lstthuadatcanhtac));

                        }
                        if (phuongthuctinh == "100000000") // tham so
                        {
                            int phuongthuctu = ((OptionSetValue)ctdiemcongtru["new_phuongthuctu"]).Value;
                            int phuongthucden = ((OptionSetValue)ctdiemcongtru["new_phuongthucden"]).Value;
                            decimal thamsotu = ctdiemcongtru.GetAttributeValue<decimal>("new_thamsotu");
                            decimal thamsoden = ctdiemcongtru.GetAttributeValue<decimal>("new_thamsoden");

                            decimal value = ((tongdientichmiatuoi / dientichtrongmiaonly) * 100);

                            if (tinhdiem(phuongthuctu, thamsotu, phuongthucden, thamsoden, value))
                            {
                                diemcong += ctdiemcongtru.GetAttributeValue<decimal>("new_diemcongtru");
                                a1 = ctdiemcongtru.GetAttributeValue<decimal>("new_diemcongtru"); ;
                            }
                        }
                    }
                    #endregion
                    else if (loaitinhdiem == "100000001") // dien tich dat nha chem ty le >= 50% tong dien tich hop dong 
                    {

                        decimal dientichdatnha = 0;
                        List<Entity> lstthuacanhtacdatnha = RetrieveMultiRecord(service, "new_thuadatcanhtac",
                                new ColumnSet(true), "new_hopdongdautumia", hdonly.Id);
                        foreach (Entity en in lstthuacanhtacdatnha)
                        {
                            Entity thuadat = service.Retrieve("new_thuadat", ((EntityReference)en["new_thuadat"]).Id, new ColumnSet(true));

                            string loaisohuudat = ((OptionSetValue)thuadat["new_loaisohuudat"]).Value.ToString();
                            if (loaisohuudat == "100000001")
                            {
                                dientichdatnha += en.GetAttributeValue<decimal>("new_dientichthucte");
                            }
                        }
                        if (phuongthuctinh == "100000000") // tham so
                        {
                            int phuongthuctu = ((OptionSetValue)ctdiemcongtru["new_phuongthuctu"]).Value;
                            int phuongthucden = ((OptionSetValue)ctdiemcongtru["new_phuongthucden"]).Value;
                            decimal thamsotu = ctdiemcongtru.GetAttributeValue<decimal>("new_thamsotu");
                            decimal thamsoden = ctdiemcongtru.GetAttributeValue<decimal>("new_thamsoden");
                            decimal value = ((dientichdatnha / dientichtrongmiaonly) * 100);

                            if (tinhdiem(phuongthuctu, thamsotu, phuongthucden, thamsoden, value))
                            {
                                diemcong = diemcong + ctdiemcongtru.GetAttributeValue<decimal>("new_diemcongtru");
                                b = ctdiemcongtru.GetAttributeValue<decimal>("new_diemcongtru");
                            }
                        }
                    }
                    else if (loaitinhdiem == "100000002") // khach hang co dien dich >= 30ha
                    {
                        if (phuongthuctinh == "100000000") // tham so
                        {
                            int phuongthuctu = ((OptionSetValue)ctdiemcongtru["new_phuongthuctu"]).Value;
                            int phuongthucden = ((OptionSetValue)ctdiemcongtru["new_phuongthucden"]).Value;
                            decimal thamsotu = ctdiemcongtru.GetAttributeValue<decimal>("new_thamsotu");
                            decimal thamsoden = ctdiemcongtru.GetAttributeValue<decimal>("new_thamsoden");

                            if (tinhdiem(phuongthuctu, thamsotu, phuongthucden, thamsoden, dientichtrongmiaonly))
                            {
                                diemcong = diemcong + ctdiemcongtru.GetAttributeValue<decimal>("new_diemcongtru");
                                c = ctdiemcongtru.GetAttributeValue<decimal>("new_diemcongtru"); ;
                            }
                        }
                    }
                    else if (loaitinhdiem == "100000003")//khach hang co sang kiến cong ty ghi nhận
                    {
                        QueryExpression qSangkienKH = new QueryExpression("new_ghinhandanhgiakhachhang");
                        q.ColumnSet = new ColumnSet(true);
                        q.Criteria = new FilterExpression();
                        q.Criteria.AddCondition(new ConditionExpression("new_khachhang", ConditionOperator.Equal, khachhangEn.Id));
                        q.Criteria.AddCondition(new ConditionExpression("new_loaighinhan", ConditionOperator.Equal, 100000000));
                        EntityCollection enSangkienKH = service.RetrieveMultiple(qSangkienKH);

                        List<Entity> lstSangkienKH = enSangkienKH.Entities.ToList<Entity>();
                        if (lstSangkienKH.Count > 0)
                        {
                            diemcong = diemcong + ctdiemcongtru.GetAttributeValue<decimal>("new_diemcongtru");
                            d = ctdiemcongtru.GetAttributeValue<decimal>("new_diemcongtru");
                        }
                    }

                    else if (loaitinhdiem == "100000004")//Có vi phạm hợp đồng và qui định của cty
                    {
                        QueryExpression qBienbanvipham = new QueryExpression("new_bienbanvipham");
                        qBienbanvipham.ColumnSet = new ColumnSet(true);
                        qBienbanvipham.Criteria = new FilterExpression();
                        qBienbanvipham.Criteria.AddCondition(new ConditionExpression("new_vudautu", ConditionOperator.Equal, vudautuhientai.Id));
                        qBienbanvipham.Criteria.AddCondition(new ConditionExpression("new_doitackh", ConditionOperator.Equal, khachhangEn.Id));
                        EntityCollection enBienbanvipham = service.RetrieveMultiple(qBienbanvipham);

                        List<Entity> lstBienbanvipham = enBienbanvipham.Entities.ToList<Entity>();
                        if (lstBienbanvipham.Count > 0)
                        {
                            diemcong = diemcong + ctdiemcongtru.GetAttributeValue<decimal>("new_diemcongtru");
                            e = ctdiemcongtru.GetAttributeValue<decimal>("new_diemcongtru");
                        }
                    }
                    else if (loaitinhdiem == "100000005")//Tỷ lệ sản lượng mía cháy /tổng sản lượng > 10% (của vụ liền kề)
                    {
                        QueryExpression qNghiemthusauthuhoach = new QueryExpression("new_nghiemthuchatsatgoc");
                        qNghiemthusauthuhoach.ColumnSet = new ColumnSet(true);
                        qNghiemthusauthuhoach.Criteria = new FilterExpression();
                        qNghiemthusauthuhoach.Criteria.AddCondition(new ConditionExpression("new_khachhang", ConditionOperator.Equal, khachhangEn.Id));

                        LinkEntity linkHopdongdautumia = new LinkEntity("new_nghiemthuchatsatgoc", "new_hopdongdautumia", "new_hopdongdautumia", "new_hopdongdautumiaid", JoinOperator.Inner);
                        qNghiemthusauthuhoach.LinkEntities.Add(linkHopdongdautumia);

                        linkHopdongdautumia.LinkCriteria = new FilterExpression();
                        linkHopdongdautumia.LinkCriteria.AddCondition(new ConditionExpression("new_vudautu", ConditionOperator.Equal, vudautuhientai.Id));
                        EntityCollection enNghiemthusauthuhoach = service.RetrieveMultiple(qNghiemthusauthuhoach);

                        int phuongthuctu = ((OptionSetValue)ctdiemcongtru["new_phuongthuctu"]).Value;
                        int phuongthucden = ((OptionSetValue)ctdiemcongtru["new_phuongthucden"]).Value;
                        decimal thamsotu = ctdiemcongtru.GetAttributeValue<decimal>("new_thamsotu");
                        decimal thamsoden = ctdiemcongtru.GetAttributeValue<decimal>("new_thamsoden");
                        //List<Entity> lstNghiemthusauthuhoach = enNghiemthusauthuhoach.Entities.ToList<Entity>();
                        if (enNghiemthusauthuhoach.Entities.ToList<Entity>().Count > 0)
                        {
                            Entity Nghiemthusauthuhoach = enNghiemthusauthuhoach.Entities.ToList<Entity>().FirstOrDefault();

                            List<Entity> lstchitietnghiemthu = RetrieveMultiRecord(service, "new_chitietnghiemthusauthuhoach", new ColumnSet(true), "new_nghiemthusauthuhoach", Nghiemthusauthuhoach.Id);

                            foreach (Entity en in lstchitietnghiemthu)
                            {
                                decimal tongslmiachay = en.GetAttributeValue<decimal>("new_miachay");
                                decimal tongsl = en.GetAttributeValue<decimal>("new_tongsanluong");
                                if (tongsl != 0)
                                {
                                    decimal phantram = (tongslmiachay / tongsl) * 100;

                                    if (tinhdiem(phuongthuctu, thamsotu, phuongthucden, thamsoden, phantram))
                                    {
                                        diemcong = diemcong + ctdiemcongtru.GetAttributeValue<decimal>("new_diemcongtru");
                                        f = ctdiemcongtru.GetAttributeValue<decimal>("new_diemcongtru");
                                    }
                                }
                            }
                        }
                        else
                        {
                            QueryExpression qVudautu1 = new QueryExpression("new_vudautu");
                            qVudautu1.ColumnSet = new ColumnSet(true);
                            qVudautu1.Criteria = new FilterExpression();
                            qVudautu1.Criteria.AddCondition(new ConditionExpression("statecode", ConditionOperator.Equal, 0));
                            EntityCollection en_qVudautu1 = service.RetrieveMultiple(qVudautu1);

                            List<Entity> lstVudautu = a.Entities.OrderBy(p => p.GetAttributeValue<DateTime>("new_ngaybatdau")).ToList<Entity>();
                            int curr1 = lst.FindIndex(p => p.Id == vudautuhientai.Id);

                            Entity vudautulienke = service.Retrieve("new_vudautu", lst[curr1 - 1].Id, new ColumnSet(true));

                            QueryExpression qNghiemthusauthuhoachlienke = new QueryExpression("new_nghiemthuchatsatgoc");
                            qNghiemthusauthuhoachlienke.ColumnSet = new ColumnSet(true);
                            qNghiemthusauthuhoachlienke.Criteria = new FilterExpression();
                            qNghiemthusauthuhoachlienke.Criteria.AddCondition(new ConditionExpression("new_khachhang", ConditionOperator.Equal, khachhangEn.Id));

                            LinkEntity linkHopdongdautumialienke = new LinkEntity("new_nghiemthuchatsatgoc", "new_hopdongdautumia", "new_hopdongdautumia", "new_hopdongdautumiaid", JoinOperator.Inner);
                            qNghiemthusauthuhoachlienke.LinkEntities.Add(linkHopdongdautumialienke);

                            linkHopdongdautumialienke.LinkCriteria = new FilterExpression();
                            linkHopdongdautumialienke.LinkCriteria.AddCondition(new ConditionExpression("new_vudautu", ConditionOperator.Equal, vudautulienke.Id));
                            EntityCollection enNghiemthusauthuhoachlienke = service.RetrieveMultiple(qNghiemthusauthuhoachlienke);
                            if (enNghiemthusauthuhoachlienke.Entities.ToList<Entity>().Count > 0)
                            {
                                Entity Nghiemthusauthuhoach = enNghiemthusauthuhoach.Entities.ToList<Entity>().FirstOrDefault();

                                List<Entity> lstchitietnghiemthu = RetrieveMultiRecord(service, "new_chitietnghiemthusauthuhoach", new ColumnSet(true), "new_nghiemthusauthuhoach", Nghiemthusauthuhoach.Id);

                                foreach (Entity en in lstchitietnghiemthu)
                                {
                                    decimal tongslmiachay = en.GetAttributeValue<decimal>("new_miachay");
                                    decimal tongsl = en.GetAttributeValue<decimal>("new_tongsanluong");
                                    if (tongsl != 0)
                                    {
                                        decimal phantram = (tongslmiachay / tongsl) * 100;

                                        if (tinhdiem(phuongthuctu, thamsotu, phuongthucden, thamsoden, phantram))
                                        {
                                            diemcong = diemcong + ctdiemcongtru.GetAttributeValue<decimal>("new_diemcongtru");
                                            f = ctdiemcongtru.GetAttributeValue<decimal>("new_diemcongtru");
                                        }
                                    }
                                }
                            }
                        }
                    }
                    else if (loaitinhdiem == "100000006")//Khách hàng có ảnh hưởng đến các khách hàng khác trong công tác phát triển vùng nguyên liệu
                    {
                        QueryExpression qSangkienKH = new QueryExpression("new_ghinhandanhgiakhachhang");
                        qSangkienKH.ColumnSet = new ColumnSet(true);
                        qSangkienKH.Criteria = new FilterExpression();
                        qSangkienKH.Criteria.AddCondition(new ConditionExpression("new_khachhang", ConditionOperator.Equal, khachhangEn.Id));
                        qSangkienKH.Criteria.AddCondition(new ConditionExpression("new_loaighinhan", ConditionOperator.Equal, 100000001));
                        EntityCollection enSangkienKH = service.RetrieveMultiple(qSangkienKH);

                        List<Entity> lstSangkienKH = enSangkienKH.Entities.ToList<Entity>();
                        if (lstSangkienKH.Count > 0)
                        {
                            diemcong = diemcong + ctdiemcongtru.GetAttributeValue<decimal>("new_diemcongtru");
                            g = ctdiemcongtru.GetAttributeValue<decimal>("new_diemcongtru");
                        }
                    }
                    else if (loaitinhdiem == "100000007")//Khả năng phát triển DT qua từng năm DT có xu hướng tăng – không giảm
                    {
                        QueryExpression qSangkienKH = new QueryExpression("new_ghinhandanhgiakhachhang");
                        qSangkienKH.ColumnSet = new ColumnSet(true);
                        qSangkienKH.Criteria = new FilterExpression();
                        qSangkienKH.Criteria.AddCondition(new ConditionExpression("new_khachhang", ConditionOperator.Equal, khachhangEn.Id));
                        qSangkienKH.Criteria.AddCondition(new ConditionExpression("new_loaighinhan", ConditionOperator.Equal, 100000002));
                        EntityCollection enSangkienKH = service.RetrieveMultiple(qSangkienKH);

                        List<Entity> lstSangkienKH = enSangkienKH.Entities.ToList<Entity>();
                        if (lstSangkienKH.Count > 0)
                        {
                            diemcong = diemcong + ctdiemcongtru.GetAttributeValue<decimal>("new_diemcongtru");
                            h = ctdiemcongtru.GetAttributeValue<decimal>("new_diemcongtru");
                        }
                    }
                }
                tongdiem = tongdiem + diemcong;
                if (tongdiem > 10)
                {
                    tongdiem = 10;
                }

                List<Entity> lstThangdiemphannhomKH = RetrieveMultiRecord(service, "new_thangdiemphannhomkhachhang", new ColumnSet(true), "new_vudautu", vudautuhientai.Id);
                Entity newKH = new Entity("contact");
                newKH = service.Retrieve(khachhangEn.LogicalName, khachhangEn.Id, new ColumnSet(true));
                int n = lstThangdiemphannhomKH.Count;
                for (int i = 0; i < n; i++)
                {
                    int phuongthuctu = ((OptionSetValue)lstThangdiemphannhomKH[i]["new_phuongthuctu"]).Value;
                    int phuongthucden = ((OptionSetValue)lstThangdiemphannhomKH[i]["new_phuongthucden"]).Value;
                    decimal thamsotu = lstThangdiemphannhomKH[i].GetAttributeValue<decimal>("new_diemtu");
                    decimal thamsoden = lstThangdiemphannhomKH[i].GetAttributeValue<decimal>("new_diemden");

                    if (tinhdiem(phuongthuctu, thamsotu, phuongthucden, thamsoden, tongdiem))
                    {
                        newKH["new_nhomkhachhang"] = lstThangdiemphannhomKH[i]["new_nhomkhachhang"];
                        newKH["new_diem"] = tongdiem;
                    }
                    if (isNoquahan == true)
                    {
                        if(i == n-1){
                            newKH["new_nhomkhachhang"] = lstThangdiemphannhomKH[i]["new_nhomkhachhang"];
                        }
                        else
                            newKH["new_nhomkhachhang"] = lstThangdiemphannhomKH[i++]["new_nhomkhachhang"];
                    }
                }

                service.Update(newKH);
                //throw new Exception(a1.ToString() + "," + b.ToString() + "," + c.ToString() + "," + d.ToString() + "," + e.ToString() + "," + f.ToString() + "," + g.ToString() + "," + h.ToString() + "," + tongdiem.ToString() + "," + nangsuatbinhquan.ToString() + "," + noquahan1.ToString());
            }
            #endregion
            //-----------------------------------------------
            else if (entrf.LogicalName == "account") // khach hang doanh nghiep
            {                
                Entity khachhangEn = this.service.Retrieve(entrf.LogicalName, entrf.Id, new ColumnSet(new string[] { "new_thoigianhoptac", "new_nangsuatbinhquan", "new_culybinhquan", "new_nangluctaichinh_diem" }));
                string khachhangEnID = khachhangEn.Id.ToString();
                decimal tongdiem = new decimal();
                decimal diemcong = new decimal();
                bool isNoquahan = false;
                Entity vudautuhientai = RetrieveSingleRecord(service, "new_vudautu", new ColumnSet(true), "new_danghoatdong", true);
                Entity tieuchiphannhomKH = RetrieveSingleRecord(service, "new_tieuchiphannhomkhachhang", new ColumnSet(new string[] { "new_tytrong_tght", "new_tytrong_nsbq", "new_tytrong_clbq", "new_tytrong_qmdt", "new_tytrong_nqh", "new_tytrong_nltc", "new_name" }), "new_vudautu", vudautuhientai.Id);

                // tinh diem tieu chi 
                QueryExpression qHddtmhientai = new QueryExpression("new_hopdongdautumia");
                qHddtmhientai.ColumnSet = new ColumnSet(true);
                qHddtmhientai.Criteria = new FilterExpression();
                qHddtmhientai.Criteria.AddCondition(new ConditionExpression("new_vudautu", ConditionOperator.Equal, vudautuhientai.Id));
                qHddtmhientai.Criteria.AddCondition(new ConditionExpression("new_khachhangdoanhnghiep", ConditionOperator.Equal, khachhangEn.Id));
                EntityCollection entcHddtm = service.RetrieveMultiple(qHddtmhientai);
                List<Entity> lstHDDTM = entcHddtm.Entities.ToList<Entity>();
                if (lstHDDTM.Count <= 0)
                {
                    throw new Exception("Khách hàng không có hợp đồng nào !!!");
                }
                if (!tieuchiphannhomKH.Contains("new_tytrong_tght"))
                    throw new Exception("Chưa có tỷ trọng thời gian hợp tác !!");
                decimal tytrongTGHT = tieuchiphannhomKH.GetAttributeValue<decimal>("new_tytrong_tght");

                if (!tieuchiphannhomKH.Contains("new_tytrong_nsbq"))
                    throw new Exception("Chưa có tỷ trọng năng suất bình quân đường !!");
                decimal tytrongNSBQD = tieuchiphannhomKH.GetAttributeValue<decimal>("new_tytrong_nsbq");

                if (!tieuchiphannhomKH.Contains("new_tytrong_clbq"))
                    throw new Exception("Chưa có tỷ trọng cự li bình quân !!");
                decimal tytrongCLBQ = tieuchiphannhomKH.GetAttributeValue<decimal>("new_tytrong_clbq");

                if (!tieuchiphannhomKH.Contains("new_tytrong_qmdt"))
                    throw new Exception("Chưa có tỷ trọng quy mô diện tích !!");
                decimal tytrongQMDT = tieuchiphannhomKH.GetAttributeValue<decimal>("new_tytrong_qmdt");

                if (!tieuchiphannhomKH.Contains("new_tytrong_nqh"))
                    throw new Exception("Chưa có tỷ trọng nợ quá hạn !!");
                decimal tytrongNQH = tieuchiphannhomKH.GetAttributeValue<decimal>("new_tytrong_nqh");

                if (!tieuchiphannhomKH.Contains("new_tytrong_nltc"))
                    throw new Exception("Chưa có tỷ trọng năng lực tài chính !!");
                decimal tytrongNLTC = tieuchiphannhomKH.GetAttributeValue<decimal>("new_tytrong_nltc");
                #region Thoigianhoptac
                QueryExpression q = new QueryExpression("new_cosotinhdiem");
                q.ColumnSet = new ColumnSet(true);
                q.Criteria = new FilterExpression();
                q.Criteria.AddCondition(new ConditionExpression("new_loaitieuchi", ConditionOperator.Equal, "100000000"));
                q.Criteria.AddCondition(new ConditionExpression("new_tieuchiphannhomkhachhang", ConditionOperator.Equal, tieuchiphannhomKH.Id));
                EntityCollection entc = service.RetrieveMultiple(q);
                List<Entity> lstCosotinhdiemTGHT = entc.Entities.ToList<Entity>();

                foreach (Entity TGHT in lstCosotinhdiemTGHT)
                {
                    int pheptinhtu = ((OptionSetValue)TGHT["new_pheptinhtu"]).Value;
                    int pheptinhden = ((OptionSetValue)TGHT["new_pheptinhden"]).Value;

                    if (!khachhangEn.Contains("new_thoigianhoptac"))
                    {
                        throw new Exception("Khách hàng doanh nghiệp chưa có thời gian hợp tác");
                    }
                    decimal tght = (decimal)(khachhangEn.GetAttributeValue<int>("new_thoigianhoptac"));

                    if (tinhdiem(pheptinhtu, TGHT.GetAttributeValue<decimal>("new_giatritu"), pheptinhden, TGHT.GetAttributeValue<decimal>("new_giatriden"), tght))
                    {
                        tongdiem = tongdiem + ((decimal)(TGHT.GetAttributeValue<int>("new_diem")) * tytrongTGHT / 100);
                    }
                }
                #endregion

                #region NSBQD
                QueryExpression q1 = new QueryExpression("new_cosotinhdiem");
                q1.ColumnSet = new ColumnSet(true);
                q1.Criteria = new FilterExpression();
                q1.Criteria.AddCondition(new ConditionExpression("new_loaitieuchi", ConditionOperator.Equal, "100000001"));
                q1.Criteria.AddCondition(new ConditionExpression("new_tieuchiphannhomkhachhang", ConditionOperator.Equal, tieuchiphannhomKH.Id));
                EntityCollection entc1 = service.RetrieveMultiple(q1);
                List<Entity> lstCosotinhdiemNSBQD = entc1.Entities.ToList<Entity>();

                QueryExpression qVudautu = new QueryExpression("new_vudautu");
                qVudautu.ColumnSet = new ColumnSet(true);
                qVudautu.Criteria = new FilterExpression();
                qVudautu.Criteria.AddCondition(new ConditionExpression("statecode", ConditionOperator.Equal, 0));
                EntityCollection a = service.RetrieveMultiple(qVudautu);

                List<Entity> lst = a.Entities.OrderBy(p => p.GetAttributeValue<DateTime>("new_ngaybatdau")).ToList<Entity>();
                int curr = lst.FindIndex(p => p.Id == vudautuhientai.Id);
                decimal nangsuatbinhquan = 0;
                List<Entity> lstHDDTM3nam = new List<Entity>();
                Entity khEditNSBQ = new Entity("account");
                if (3 <= curr)
                {
                    for (int i = 1; i <= 3; i++)
                    {
                        QueryExpression qHddtm3nam = new QueryExpression("new_hopdongdautumia");
                        qHddtmhientai.ColumnSet = new ColumnSet(true);
                        qHddtmhientai.Criteria = new FilterExpression();
                        qHddtmhientai.Criteria.AddCondition(new ConditionExpression("new_vudautu", ConditionOperator.Equal, lst[curr - i].Id));
                        qHddtmhientai.Criteria.AddCondition(new ConditionExpression("new_khachhangdoanhnghiep", ConditionOperator.Equal, khachhangEn.Id));
                        EntityCollection entcHddtm3nam = service.RetrieveMultiple(qHddtmhientai);
                        lstHDDTM3nam = entcHddtm3nam.Entities.ToList<Entity>();

                        nangsuatbinhquan += sumnangsuatbinhquan(new EntityCollection(lstHDDTM3nam));
                        //foreach (Entity en in lstHDDTM3nam)
                        //{
                        //    List<Entity> Lstthuadatcanhtac = RetrieveMultiRecord(service, "new_thuadatcanhtac", new ColumnSet(new string[] { "new_nangsuatbinhquan", "new_name" }), "new_hopdongdautumia", en.Id);
                        //    nangsuatbinhquan = nangsuatbinhquan + sumnangsuatbinhquan(new EntityCollection(Lstthuadatcanhtac));
                        //}
                    }


                }
                else if (curr == 2)
                {
                    for (int i = 1; i <= 2; i++)
                    {
                        QueryExpression qHddtm3nam = new QueryExpression("new_hopdongdautumia");
                        qHddtmhientai.ColumnSet = new ColumnSet(true);
                        qHddtmhientai.Criteria = new FilterExpression();
                        qHddtmhientai.Criteria.AddCondition(new ConditionExpression("new_vudautu", ConditionOperator.Equal, lst[curr - i].Id));
                        qHddtmhientai.Criteria.AddCondition(new ConditionExpression("new_khachhangdoanhnghiep", ConditionOperator.Equal, khachhangEn.Id));
                        EntityCollection entcHddtm3nam = service.RetrieveMultiple(qHddtmhientai);
                        lstHDDTM3nam = entcHddtm3nam.Entities.ToList<Entity>();
                        nangsuatbinhquan = nangsuatbinhquan + sumnangsuatbinhquan(new EntityCollection(lstHDDTM3nam));

                        //foreach (Entity en in lstHDDTM3nam)
                        //{
                        //    List<Entity> Lstthuadatcanhtac = RetrieveMultiRecord(service, "new_thuadatcanhtac", new ColumnSet(new string[] { "new_nangsuatbinhquan", "new_name" }), "new_hopdongdautumia", en.Id);
                        //    nangsuatbinhquan = nangsuatbinhquan + sumnangsuatbinhquan(new EntityCollection(Lstthuadatcanhtac));
                        //}
                    }
                }
                else if (curr == 1)
                {
                    QueryExpression qHddtm3nam = new QueryExpression("new_hopdongdautumia");
                    qHddtmhientai.ColumnSet = new ColumnSet(true);
                    qHddtmhientai.Criteria = new FilterExpression();
                    qHddtmhientai.Criteria.AddCondition(new ConditionExpression("new_vudautu", ConditionOperator.Equal, lst[curr - 1].Id));
                    qHddtmhientai.Criteria.AddCondition(new ConditionExpression("new_khachhangdoanhnghiep", ConditionOperator.Equal, khachhangEn.Id));
                    EntityCollection entcHddtm3nam = service.RetrieveMultiple(qHddtmhientai);
                    Entity temp = entcHddtm3nam.Entities.ToList<Entity>().FirstOrDefault();

                    nangsuatbinhquan = temp.Contains("new_nangsuatduong") ? temp.GetAttributeValue<decimal>("new_nangsuatbinhquan") : 0;
                }

                khEditNSBQ = service.Retrieve(khachhangEn.LogicalName, khachhangEn.Id, new ColumnSet(new string[] { "new_nangsuatbinhquan", "new_makhachhang" }));
                khEditNSBQ["new_nangsuatbinhquan"] = nangsuatbinhquan;

                service.Update(khEditNSBQ);

                foreach (Entity NSBQD in lstCosotinhdiemNSBQD)
                {
                    int pheptinhtu = ((OptionSetValue)NSBQD["new_pheptinhtu"]).Value;
                    int pheptinhden = ((OptionSetValue)NSBQD["new_pheptinhden"]).Value;

                    if (!khachhangEn.Contains("new_nangsuatduong"))
                    {
                        throw new Exception("Khách hàng chưa có năng suất đường");
                    }
                    //decimal nangsuatduong = (decimal)(khachhangEn.GetAttributeValue<decimal>("new_nangsuatduong"));

                    if (tinhdiem(pheptinhtu, NSBQD.GetAttributeValue<decimal>("new_giatritu"), pheptinhden, NSBQD.GetAttributeValue<decimal>("new_giatriden"), nangsuatbinhquan))
                    {
                        tongdiem = tongdiem + (decimal)(NSBQD.GetAttributeValue<int>("new_diem")) * tytrongNSBQD / 100;
                    }
                }

                #endregion
                #region Culibinhquan
                QueryExpression q2 = new QueryExpression("new_cosotinhdiem");
                q2.ColumnSet = new ColumnSet(true);
                q2.Criteria = new FilterExpression();
                q2.Criteria.AddCondition(new ConditionExpression("new_loaitieuchi", ConditionOperator.Equal, "100000002"));
                q2.Criteria.AddCondition(new ConditionExpression("new_tieuchiphannhomkhachhang", ConditionOperator.Equal, tieuchiphannhomKH.Id));
                EntityCollection entc2 = service.RetrieveMultiple(q2);
                List<Entity> lstCosotinhdiemCLBQ = entc2.Entities.ToList<Entity>();
                foreach (Entity CLBQ in lstCosotinhdiemCLBQ)
                {
                    int pheptinhtu = ((OptionSetValue)CLBQ["new_pheptinhtu"]).Value;
                    int pheptinhden = ((OptionSetValue)CLBQ["new_pheptinhden"]).Value;

                    if (!khachhangEn.Contains("new_culybinhquan"))
                    {
                        throw new Exception("Khách hàng doanh nghiệp chưa có cự li bình quân !");
                    }
                    decimal clbq = khachhangEn.GetAttributeValue<decimal>("new_culybinhquan");

                    if (tinhdiem(pheptinhtu, CLBQ.GetAttributeValue<decimal>("new_giatritu"), pheptinhden, CLBQ.GetAttributeValue<decimal>("new_giatriden"), clbq))
                    {
                        tongdiem = tongdiem + (decimal)(CLBQ.GetAttributeValue<int>("new_diem")) * tytrongCLBQ / 100;
                    }
                }
                #endregion
                #region Quimodientich
                QueryExpression q3 = new QueryExpression("new_cosotinhdiem");
                q3.ColumnSet = new ColumnSet(true);
                q3.Criteria = new FilterExpression();
                q3.Criteria.AddCondition(new ConditionExpression("new_loaitieuchi", ConditionOperator.Equal, "100000003"));
                q3.Criteria.AddCondition(new ConditionExpression("new_tieuchiphannhomkhachhang", ConditionOperator.Equal, tieuchiphannhomKH.Id));
                EntityCollection entc3 = service.RetrieveMultiple(q3);
                List<Entity> lstCosotinhdiemQMDT = entc3.Entities.ToList<Entity>();
                decimal tongdientich = 0;

                foreach (Entity hopdongdautumia in lstHDDTM)
                {
                    List<Entity> Lstthuadatcanhtac = RetrieveMultiRecord(service, "new_thuadatcanhtac",
                        new ColumnSet(new string[] { "new_dientichthucte" }), "new_hopdongdautumia", hopdongdautumia.Id);
                    tongdientich = tongdientich + sumdientichthucte(new EntityCollection(Lstthuadatcanhtac));
                }

                foreach (Entity QMDT in lstCosotinhdiemQMDT)
                {
                    int pheptinhtu = ((OptionSetValue)QMDT["new_pheptinhtu"]).Value;
                    int pheptinhden = ((OptionSetValue)QMDT["new_pheptinhden"]).Value;

                    if (tinhdiem(pheptinhtu, QMDT.GetAttributeValue<decimal>("new_giatritu"), pheptinhden, QMDT.GetAttributeValue<decimal>("new_giatriden"), tongdientich))
                    {
                        tongdiem = tongdiem + (decimal)(QMDT.GetAttributeValue<int>("new_diem")) * tytrongQMDT / 100;
                    }
                }

                #endregion
                #region Noquahan
                QueryExpression q4 = new QueryExpression("new_cosotinhdiem");
                q4.ColumnSet = new ColumnSet(true);
                q4.Criteria = new FilterExpression();
                q4.Criteria.AddCondition(new ConditionExpression("new_loaitieuchi", ConditionOperator.Equal, "100000004"));
                q4.Criteria.AddCondition(new ConditionExpression("new_tieuchiphannhomkhachhang", ConditionOperator.Equal, tieuchiphannhomKH.Id));
                EntityCollection entc4 = service.RetrieveMultiple(q4);
                List<Entity> lstCosotinhdiemNQH = entc4.Entities.ToList<Entity>();
                decimal noquahan = 0;
                QueryExpression q5 = new QueryExpression("new_phanbodautu");
                q5.ColumnSet = new ColumnSet(true);
                q5.Criteria = new FilterExpression();
                q5.Criteria.AddCondition(new ConditionExpression("new_vudautu", ConditionOperator.Equal, vudautuhientai.Id));
                q5.Criteria.AddCondition(new ConditionExpression("new_khachhangdoanhnghiep", ConditionOperator.Equal, khachhangEn.Id));
                EntityCollection entc5 = service.RetrieveMultiple(q5);
                List<Entity> lstPhanbodautu = entc5.Entities.ToList<Entity>();

                foreach (Entity pbdt in lstPhanbodautu)
                {
                    if (pbdt.GetAttributeValue<DateTime>("new_hanthanhtoan") < DateTime.Today)
                    {
                        isNoquahan = true;
                        noquahan = noquahan + ((Money)pbdt["new_conlai"]).Value;
                    }
                }

                foreach (Entity NQH in lstCosotinhdiemNQH)
                {
                    int pheptinhtu = ((OptionSetValue)NQH["new_pheptinhtu"]).Value;
                    int pheptinhden = ((OptionSetValue)NQH["new_pheptinhden"]).Value;
                    string cachtinh = ((OptionSetValue)NQH["new_cachtinh"]).Value.ToString();
                    decimal giatritu = 0;
                    decimal giatriden = 0;

                    if (cachtinh == "100000001")
                    {
                        giatritu = ((Money)NQH["new_sotientu"]).Value;
                        giatriden = ((Money)NQH["new_sotienden"]).Value;
                    }
                    else
                    {
                        giatritu = NQH.GetAttributeValue<decimal>("new_giatritu");
                        giatriden = NQH.GetAttributeValue<decimal>("new_giatriden");
                    }

                    if (tinhdiem(pheptinhtu, giatritu, pheptinhden, giatriden, noquahan))
                    {

                        tongdiem = tongdiem + (decimal)(NQH.GetAttributeValue<int>("new_diem")) * tytrongNQH / 100;
                    }
                }
                #endregion
                #region Nangluctaichinh
                QueryExpression q6 = new QueryExpression("new_cosotinhdiem");
                q6.ColumnSet = new ColumnSet(true);
                q6.Criteria = new FilterExpression();
                q6.Criteria.AddCondition(new ConditionExpression("new_loaitieuchi", ConditionOperator.Equal, "100000005"));
                q6.Criteria.AddCondition(new ConditionExpression("new_tieuchiphannhomkhachhang", ConditionOperator.Equal, tieuchiphannhomKH.Id));
                EntityCollection entc6 = service.RetrieveMultiple(q6);
                List<Entity> lstCosotinhdiemNLTC = entc6.Entities.ToList<Entity>();
                foreach (Entity NLTC in lstCosotinhdiemNLTC)
                {
                    int pheptinhtu = ((OptionSetValue)NLTC["new_pheptinhtu"]).Value;
                    int pheptinhden = ((OptionSetValue)NLTC["new_pheptinhden"]).Value;

                    if (!khachhangEn.Contains("new_nangluctaichinh_diem"))
                    {
                        throw new Exception("Khách hàng doanh nghiệp chưa có năng lực tài chính");
                    }

                    decimal nltc = (decimal)(khachhangEn.GetAttributeValue<int>("new_nangluctaichinh_diem"));

                    if (tinhdiem(pheptinhtu, NLTC.GetAttributeValue<decimal>("new_giatritu"), pheptinhden, NLTC.GetAttributeValue<decimal>("new_giatriden"), nltc))
                    {
                        tongdiem = tongdiem + ((decimal)(NLTC.GetAttributeValue<int>("new_diem")) * tytrongNLTC / 100);
                    }
                }

                #endregion
                //tinh diem cong tru
                List<Entity> lstCachtinhdiemcongtru = RetrieveMultiRecord(service, "new_cachtinhdiemcongtru", new ColumnSet(true), "new_tieuchiphannhomkh", tieuchiphannhomKH.Id);
                foreach (Entity ctdiemcongtru in lstCachtinhdiemcongtru)
                {
                    decimal tongdientichmiatuoi = 0;
                    decimal dientichtrongmia = 0;
                    Entity hdonly = lstHDDTM.FirstOrDefault();
                    decimal dientichtrongmiaonly = hdonly.GetAttributeValue<decimal>("new_dientichtrongmia");
                    string loaitinhdiem = ((OptionSetValue)ctdiemcongtru["new_loaitinhdiem"]).Value.ToString();
                    string phuongthuctinh = ((OptionSetValue)ctdiemcongtru["new_phuongthuctinh"]).Value.ToString();
                    #region dien tich tuoi mia huu hieu > 30% dien tich hop dong
                    if (loaitinhdiem == "100000000")//dien tich tuoi mia huu hieu > 30% dien tich hop dong 
                    {
                        foreach (Entity hopdongdautumia in lstHDDTM)
                        {
                            if (hopdongdautumia.Contains("new_dientichtrongmia"))
                            {
                                dientichtrongmia += hopdongdautumia.GetAttributeValue<decimal>("new_dientichtrongmia");
                            }
                            List<Entity> Lstthuadatcanhtac = RetrieveMultiRecord(service, "new_thuadatcanhtac",
                                new ColumnSet(new string[] { "new_dientichmiatuoi" }), "new_hopdongdautumia", hopdongdautumia.Id);
                            tongdientichmiatuoi = tongdientichmiatuoi + sumdientichmiatuoi(new EntityCollection(Lstthuadatcanhtac));

                        }
                        if (phuongthuctinh == "100000000") // tham so
                        {
                            int phuongthuctu = ((OptionSetValue)ctdiemcongtru["new_phuongthuctu"]).Value;
                            int phuongthucden = ((OptionSetValue)ctdiemcongtru["new_phuongthucden"]).Value;
                            decimal thamsotu = ctdiemcongtru.GetAttributeValue<decimal>("new_thamsotu");
                            decimal thamsoden = ctdiemcongtru.GetAttributeValue<decimal>("new_thamsoden");
                            decimal value = ((tongdientichmiatuoi / dientichtrongmiaonly) * 100);

                            if (tinhdiem(phuongthuctu, thamsotu, phuongthucden, thamsoden, value))
                            {
                                diemcong += ctdiemcongtru.GetAttributeValue<decimal>("new_diemcongtru");
                            }
                        }
                    }
                    #endregion
                    else if (loaitinhdiem == "100000001") // dien tich dat nha chem ty le >= 50% tong dien tich hop dong 
                    {
                        decimal dientichdatnha = 0;
                        List<Entity> lstthuacanhtacdatnha = RetrieveMultiRecord(service, "new_thuadatcanhtac",
                                new ColumnSet(true), "new_hopdongdautumia", hdonly.Id);
                        foreach (Entity en in lstthuacanhtacdatnha)
                        {
                            Entity thuadat = service.Retrieve("new_thuadat", ((EntityReference)en["new_thuadat"]).Id, new ColumnSet(true));

                            string loaisohuudat = ((OptionSetValue)thuadat["new_loaisohuudat"]).Value.ToString();
                            if (loaisohuudat == "100000001")
                            {
                                dientichdatnha += en.GetAttributeValue<decimal>("new_dientichthucte");
                            }
                        }
                        if (phuongthuctinh == "100000000") // tham so
                        {
                            int phuongthuctu = ((OptionSetValue)ctdiemcongtru["new_phuongthuctu"]).Value;
                            int phuongthucden = ((OptionSetValue)ctdiemcongtru["new_phuongthucden"]).Value;
                            decimal thamsotu = ctdiemcongtru.GetAttributeValue<decimal>("new_thamsotu");
                            decimal thamsoden = ctdiemcongtru.GetAttributeValue<decimal>("new_thamsoden");
                            decimal value = ((dientichdatnha / dientichtrongmiaonly) * 100);

                            if (tinhdiem(phuongthuctu, thamsotu, phuongthucden, thamsoden, value))
                            {
                                diemcong = diemcong + ctdiemcongtru.GetAttributeValue<decimal>("new_diemcongtru");
                            }
                        }
                    }
                    else if (loaitinhdiem == "100000002") // khach hang co dien dich >= 30ha
                    {
                        if (phuongthuctinh == "100000000") // tham so
                        {
                            int phuongthuctu = ((OptionSetValue)ctdiemcongtru["new_phuongthuctu"]).Value;
                            int phuongthucden = ((OptionSetValue)ctdiemcongtru["new_phuongthucden"]).Value;
                            decimal thamsotu = ctdiemcongtru.GetAttributeValue<decimal>("new_thamsotu");
                            decimal thamsoden = ctdiemcongtru.GetAttributeValue<decimal>("new_thamsoden");

                            if (tinhdiem(phuongthuctu, thamsotu, phuongthucden, thamsoden, dientichtrongmiaonly))
                            {
                                diemcong = diemcong + ctdiemcongtru.GetAttributeValue<decimal>("new_diemcongtru");
                            }
                        }
                    }
                    else if (loaitinhdiem == "100000003")//khach hang co sang kiến cong ty ghi nhận
                    {
                        QueryExpression qSangkienKH = new QueryExpression("new_ghinhandanhgiakhachhang");
                        q.ColumnSet = new ColumnSet(true);
                        q.Criteria = new FilterExpression();
                        q.Criteria.AddCondition(new ConditionExpression("new_khachhang", ConditionOperator.Equal, khachhangEn.Id));
                        q.Criteria.AddCondition(new ConditionExpression("new_loaighinhan", ConditionOperator.Equal, 100000000));
                        EntityCollection enSangkienKH = service.RetrieveMultiple(qSangkienKH);

                        List<Entity> lstSangkienKH = enSangkienKH.Entities.ToList<Entity>();
                        if (lstSangkienKH.Count > 0)
                        {
                            diemcong = diemcong + ctdiemcongtru.GetAttributeValue<decimal>("new_diemcongtru");
                        }
                    }

                    else if (loaitinhdiem == "100000004")//Có vi phạm hợp đồng và qui định của cty
                    {
                        QueryExpression qBienbanvipham = new QueryExpression("new_bienbanvipham");
                        qBienbanvipham.ColumnSet = new ColumnSet(true);
                        qBienbanvipham.Criteria = new FilterExpression();
                        qBienbanvipham.Criteria.AddCondition(new ConditionExpression("new_vudautu", ConditionOperator.Equal, vudautuhientai.Id));
                        qBienbanvipham.Criteria.AddCondition(new ConditionExpression("new_doitackh", ConditionOperator.Equal, khachhangEn.Id));
                        EntityCollection enBienbanvipham = service.RetrieveMultiple(qBienbanvipham);

                        List<Entity> lstBienbanvipham = enBienbanvipham.Entities.ToList<Entity>();
                        if (lstBienbanvipham.Count > 0)
                        {
                            diemcong = diemcong + ctdiemcongtru.GetAttributeValue<decimal>("new_diemcongtru");
                        }
                    }
                    else if (loaitinhdiem == "100000005")//Tỷ lệ sản lượng mía cháy /tổng sản lượng > 10% (của vụ liền kề)
                    {
                        QueryExpression qNghiemthusauthuhoach = new QueryExpression("new_nghiemthuchatsatgoc");
                        qNghiemthusauthuhoach.ColumnSet = new ColumnSet(true);
                        qNghiemthusauthuhoach.Criteria = new FilterExpression();
                        qNghiemthusauthuhoach.Criteria.AddCondition(new ConditionExpression("new_khachhang", ConditionOperator.Equal, khachhangEn.Id));

                        LinkEntity linkHopdongdautumia = new LinkEntity("new_nghiemthuchatsatgoc", "new_hopdongdautumia", "new_hopdongdautumia", "new_hopdongdautumiaid", JoinOperator.Inner);
                        qNghiemthusauthuhoach.LinkEntities.Add(linkHopdongdautumia);

                        linkHopdongdautumia.LinkCriteria = new FilterExpression();
                        linkHopdongdautumia.LinkCriteria.AddCondition(new ConditionExpression("new_vudautu", ConditionOperator.Equal, vudautuhientai.Id));
                        EntityCollection enNghiemthusauthuhoach = service.RetrieveMultiple(qNghiemthusauthuhoach);

                        int phuongthuctu = ((OptionSetValue)ctdiemcongtru["new_phuongthuctu"]).Value;
                        int phuongthucden = ((OptionSetValue)ctdiemcongtru["new_phuongthucden"]).Value;
                        decimal thamsotu = ctdiemcongtru.GetAttributeValue<decimal>("new_thamsotu");
                        decimal thamsoden = ctdiemcongtru.GetAttributeValue<decimal>("new_thamsoden");
                        //List<Entity> lstNghiemthusauthuhoach = enNghiemthusauthuhoach.Entities.ToList<Entity>();
                        if (enNghiemthusauthuhoach.Entities.ToList<Entity>().Count > 0)
                        {
                            Entity Nghiemthusauthuhoach = enNghiemthusauthuhoach.Entities.ToList<Entity>().FirstOrDefault();

                            List<Entity> lstchitietnghiemthu = RetrieveMultiRecord(service, "new_chitietnghiemthusauthuhoach", new ColumnSet(true), "new_nghiemthusauthuhoach", Nghiemthusauthuhoach.Id);

                            foreach (Entity en in lstchitietnghiemthu)
                            {
                                decimal tongslmiachay = en.GetAttributeValue<decimal>("new_miachay");
                                decimal tongsl = en.GetAttributeValue<decimal>("new_tongsanluong");
                                if (tongsl != 0)
                                {
                                    decimal phantram = (tongslmiachay / tongsl) * 100;

                                    if (tinhdiem(phuongthuctu, thamsotu, phuongthucden, thamsoden, phantram))
                                    {
                                        diemcong = diemcong + ctdiemcongtru.GetAttributeValue<decimal>("new_diemcongtru");
                                    }
                                }
                            }
                        }
                        else
                        {
                            QueryExpression qVudautu1 = new QueryExpression("new_vudautu");
                            qVudautu1.ColumnSet = new ColumnSet(true);
                            qVudautu1.Criteria = new FilterExpression();
                            qVudautu1.Criteria.AddCondition(new ConditionExpression("statecode", ConditionOperator.Equal, 0));
                            EntityCollection en_qVudautu1 = service.RetrieveMultiple(qVudautu1);

                            List<Entity> lstVudautu = a.Entities.OrderBy(p => p.GetAttributeValue<DateTime>("new_ngaybatdau")).ToList<Entity>();
                            int curr1 = lst.FindIndex(p => p.Id == vudautuhientai.Id);

                            Entity vudautulienke = lst[curr1 - 1];

                            QueryExpression qNghiemthusauthuhoachlienke = new QueryExpression("new_nghiemthuchatsatgoc");
                            qNghiemthusauthuhoachlienke.ColumnSet = new ColumnSet(true);
                            qNghiemthusauthuhoachlienke.Criteria = new FilterExpression();
                            qNghiemthusauthuhoachlienke.Criteria.AddCondition(new ConditionExpression("new_khachhang", ConditionOperator.Equal, khachhangEn.Id));

                            LinkEntity linkHopdongdautumialienke = new LinkEntity("new_nghiemthuchatsatgoc", "new_hopdongdautumia", "new_hopdongdautumia", "new_hopdongdautumiaid", JoinOperator.Inner);
                            qNghiemthusauthuhoach.LinkEntities.Add(linkHopdongdautumialienke);

                            linkHopdongdautumialienke.LinkCriteria = new FilterExpression();
                            linkHopdongdautumialienke.LinkCriteria.AddCondition(new ConditionExpression("new_vudautu", ConditionOperator.Equal, vudautulienke.Id));
                            EntityCollection enNghiemthusauthuhoachlienke = service.RetrieveMultiple(qNghiemthusauthuhoachlienke);

                            Entity Nghiemthusauthuhoach = enNghiemthusauthuhoach.Entities.ToList<Entity>().FirstOrDefault();

                            List<Entity> lstchitietnghiemthu = RetrieveMultiRecord(service, "new_chitietnghiemthusauthuhoach", new ColumnSet(true), "new_nghiemthusauthuhoach", Nghiemthusauthuhoach.Id);

                            foreach (Entity en in lstchitietnghiemthu)
                            {
                                decimal tongslmiachay = en.GetAttributeValue<decimal>("new_miachay");
                                decimal tongsl = en.GetAttributeValue<decimal>("new_tongsanluong");
                                if (tongsl != 0)
                                {
                                    decimal phantram = (tongslmiachay / tongsl) * 100;

                                    if (tinhdiem(phuongthuctu, thamsotu, phuongthucden, thamsoden, phantram))
                                    {
                                        diemcong = diemcong + ctdiemcongtru.GetAttributeValue<decimal>("new_diemcongtru");
                                    }
                                }
                            }
                        }
                    }
                    else if (loaitinhdiem == "100000006")//Khách hàng có ảnh hưởng đến các khách hàng khác trong công tác phát triển vùng nguyên liệu
                    {
                        QueryExpression qSangkienKH = new QueryExpression("new_ghinhandanhgiakhachhang");
                        qSangkienKH.ColumnSet = new ColumnSet(true);
                        qSangkienKH.Criteria = new FilterExpression();
                        qSangkienKH.Criteria.AddCondition(new ConditionExpression("new_khachhangdoanhnghiep", ConditionOperator.Equal, khachhangEn.Id));
                        qSangkienKH.Criteria.AddCondition(new ConditionExpression("new_loaighinhan", ConditionOperator.Equal, 100000001));
                        EntityCollection enSangkienKH = service.RetrieveMultiple(qSangkienKH);

                        List<Entity> lstSangkienKH = enSangkienKH.Entities.ToList<Entity>();
                        if (lstSangkienKH.Count > 0)
                        {
                            diemcong = diemcong + ctdiemcongtru.GetAttributeValue<decimal>("new_diemcongtru");
                        }
                    }
                    else if (loaitinhdiem == "100000007")//Khả năng phát triển DT qua từng năm DT có xu hướng tăng – không giảm
                    {
                        QueryExpression qSangkienKH = new QueryExpression("new_ghinhandanhgiakhachhang");
                        qSangkienKH.ColumnSet = new ColumnSet(true);
                        qSangkienKH.Criteria = new FilterExpression();
                        qSangkienKH.Criteria.AddCondition(new ConditionExpression("new_khachhangdoanhnghiep", ConditionOperator.Equal, khachhangEn.Id));
                        qSangkienKH.Criteria.AddCondition(new ConditionExpression("new_loaighinhan", ConditionOperator.Equal, 100000002));
                        EntityCollection enSangkienKH = service.RetrieveMultiple(qSangkienKH);

                        List<Entity> lstSangkienKH = enSangkienKH.Entities.ToList<Entity>();
                        if (lstSangkienKH.Count > 0)
                        {
                            diemcong = diemcong + ctdiemcongtru.GetAttributeValue<decimal>("new_diemcongtru");
                        }
                    }
                }
                tongdiem = tongdiem + diemcong;

                if (tongdiem > 10)
                {
                    tongdiem = 10;
                }

                List<Entity> lstThangdiemphannhomKH = RetrieveMultiRecord(service, "new_thangdiemphannhomkhachhang", new ColumnSet(true), "new_vudautu", vudautuhientai.Id);
                Entity newKH = new Entity("account");
                newKH = service.Retrieve(khachhangEn.LogicalName, khachhangEn.Id, new ColumnSet(true));
                int n = lstThangdiemphannhomKH.Count;
                for (int i = 0; i < n; i++)
                {
                    int phuongthuctu = ((OptionSetValue)lstThangdiemphannhomKH[i]["new_phuongthuctu"]).Value;
                    int phuongthucden = ((OptionSetValue)lstThangdiemphannhomKH[i]["new_phuongthucden"]).Value;
                    decimal thamsotu = lstThangdiemphannhomKH[i].GetAttributeValue<decimal>("new_diemtu");
                    decimal thamsoden = lstThangdiemphannhomKH[i].GetAttributeValue<decimal>("new_diemden");

                    if (tinhdiem(phuongthuctu, thamsotu, phuongthucden, thamsoden, tongdiem))
                    {
                        newKH["new_nhomkhachhang"] = lstThangdiemphannhomKH[i]["new_nhomkhachhang"];
                        newKH["new_diem"] = tongdiem;
                    }
                    if (isNoquahan == true)
                    {
                        if (i == n - 1)
                        {
                            newKH["new_nhomkhachhang"] = lstThangdiemphannhomKH[i]["new_nhomkhachhang"];
                        }
                        else
                            newKH["new_nhomkhachhang"] = lstThangdiemphannhomKH[i++]["new_nhomkhachhang"];
                    }
                }
                service.Update(newKH);
            }
        }
        Entity FindVudautu()
        {
            Entity CurrVudautu = RetrieveSingleRecord(service, "new_vudautu", new ColumnSet(true), "new_danghoatdong", true);
            //Entity CurrVudautu = service.Retrieve("new_vudautu", vudautuRef.Id, new ColumnSet(true));
            QueryExpression q = new QueryExpression("new_vudautu");
            q.ColumnSet = new ColumnSet(true);
            q.Criteria = new FilterExpression();
            q.Criteria.AddCondition(new ConditionExpression("statecode", ConditionOperator.Equal, 0));
            EntityCollection entc = service.RetrieveMultiple(q);

            List<Entity> lst = entc.Entities.OrderBy(p => p.GetAttributeValue<DateTime>("new_ngaybatdau")).ToList<Entity>();
            int curr = lst.FindIndex(p => p.Id == CurrVudautu.Id);

            return lst[curr - 3];

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
        decimal sumdientichthucte(EntityCollection thuacanhtac)
        {
            decimal temp = 0;
            foreach (Entity en in thuacanhtac.Entities)
            {
                temp += en.Contains("new_dientichthucte") ? en.GetAttributeValue<decimal>("new_dientichthucte") : 0;
            }

            return temp;
        }

        decimal sumdientichmiatuoi(EntityCollection thuacanhtac)
        {
            decimal temp = 0;
            foreach (Entity en in thuacanhtac.Entities)
            {
                temp += en.Contains("new_dientichmiatuoi") ? en.GetAttributeValue<decimal>("new_dientichmiatuoi") : 0;
            }

            return temp;
        }
        decimal sumnangsuatbinhquan(EntityCollection thuacanhtac)
        {
            decimal temp = 0;
            foreach (Entity en in thuacanhtac.Entities)
            {
                temp += en.Contains("new_nangsuatduong") ? en.GetAttributeValue<decimal>("new_nangsuatduong") : 0;
            }

            return temp;
        }

    }
}
