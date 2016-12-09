using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk.Messages;

namespace Plugin_ManualPhieuTinhLai
{
    public class Plugin_ManualPhieuTinhLai : IPlugin
    {
        IOrganizationService service = null;
        IOrganizationServiceFactory factory = null;

        public Entity VuDauTu;
        public Entity PDNThuNo;
        public Dictionary<int, List<Entity>> Banglaisuat = new Dictionary<int, List<Entity>>(); //{loai lai suat; danh sach lai}
        public int CachTangLai = 100000000;

        void IPlugin.Execute(IServiceProvider serviceProvider)
        {
            IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
            service = factory.CreateOrganizationService(context.UserId);

            if (context.Depth == 1)
            {
                Banglaisuat = new Dictionary<int, List<Entity>>();
                CachTangLai = 100000000;
                Entity target = (Entity)context.InputParameters["Target"];
                if (context.MessageName == "Create" || (context.MessageName == "Update" && (target.Contains("new_phanbodautu") || target.Contains("new_tienvay"))))
                {
                    Entity result = new Entity("new_phieutinhlai");
                    Entity fullImage = (Entity)context.PostEntityImages["PostImg"];
                    result.Id = fullImage.Id;
                    PDNThuNo = service.Retrieve("new_phieudenghithuno", ((EntityReference)fullImage["new_phieudenghithuno"]).Id, new ColumnSet(true));
                    bool TinhTay = PDNThuNo.Contains("new_tinhtay") ? (bool)PDNThuNo["new_tinhtay"] : false;
                    // if (((OptionSetValue)PDNThuNo["new_loaithuno"]).Value == 100000001 || !TinhTay)
                    if (!((((OptionSetValue)PDNThuNo["new_loaithuno"]).Value == 100000001 && TinhTay) || ((OptionSetValue)PDNThuNo["new_loaithuno"]).Value == 100000000))
                        return;

                    Entity PBDT = service.Retrieve("new_phanbodautu", ((EntityReference)fullImage["new_phanbodautu"]).Id, new ColumnSet(true));

                    decimal ThuNoGoc = PDNThuNo.Contains("new_thunogoc") ? ((Money)PDNThuNo["new_thunogoc"]).Value : (decimal)0;
                    decimal ThuTongTien = PDNThuNo.Contains("new_tongtienthu") ? ((Money)PDNThuNo["new_tongtienthu"]).Value : (decimal)0;

                    int HinhThucTra = ((OptionSetValue)PDNThuNo["new_hinhthuctra"]).Value;
                    DateTime GioMoc = (DateTime)PDNThuNo["new_ngaythu"];

                    result["new_ngayvay"] = PBDT["new_ngayphatsinh"];
                    result["new_ngaytra"] = PDNThuNo["new_ngaythu"];

                    decimal Sotien = 0;
                    if (fullImage.Contains("new_tienvay") && ((Money)fullImage["new_tienvay"]).Value > 0)
                        Sotien = ((Money)fullImage["new_tienvay"]).Value;
                    else Sotien = (PBDT.Contains("new_conlai") ? ((Money)PBDT["new_conlai"]).Value : (decimal)0);

                    if (Sotien == 0 || (HinhThucTra == 100000001 && ThuTongTien == 0) || (HinhThucTra == 100000000 && ThuNoGoc == 0))
                    {
                        result["new_tienvay"] = new Money((decimal)0);
                        result["new_tienlai"] = new Money((decimal)0);
                        result["new_songay"] = ((DateTime)PDNThuNo["new_ngaythu"] - (DateTime)PBDT["new_ngayphatsinh"]).Days;

                        service.Update(result);
                    }
                    else
                    {
                        VuDauTu = service.Retrieve("new_vudautu", ((EntityReference)PDNThuNo["new_vudautu"]).Id, new ColumnSet("new_loaitrichthu", "new_giatri", "new_namtaichinhh", "new_cachtinhlai", "new_thutuuutien", "new_hinhthuctanglai"));
                        CachTangLai = VuDauTu.Contains("new_hinhthuctanglai") ? ((OptionSetValue)VuDauTu["new_hinhthuctanglai"]).Value : 100000000;
                        loadBangLai();

                        decimal sumgoc = 0;
                        decimal sumlai = 0;
                        GetSumPDNThuNo(PDNThuNo.Id, fullImage.Id, ref sumgoc, ref sumlai);

                        decimal sotiengoc = Sotien;
                        if (HinhThucTra == 100000000)
                            sotiengoc = ((ThuNoGoc - sumgoc) < Sotien ? (ThuNoGoc - sumgoc) : Sotien);

                        decimal totallai = 0;

                        List<Entity> DoanTinhLai = new List<Entity>();

                        switch (((OptionSetValue)VuDauTu["new_cachtinhlai"]).Value)
                        {
                            case 100000000: // lai tren tien thu
                                {
                                    Guid tmpVuDT = ((EntityReference)PBDT["new_vudautu"]).Id;

                                    decimal defaultMucLai = (decimal)PBDT["new_laisuat"];

                                    if (((OptionSetValue)PBDT["new_loailaisuat"]).Value == 100000000 || !Banglaisuat.ContainsKey(((OptionSetValue)PBDT["new_mucdichdautu"]).Value)) // cố định - Không bảng lãi
                                    {
                                        Entity dtl_tmp = new Entity("new_doantinhlai");
                                        dtl_tmp["new_name"] = string.Format("Lãi từ ngày {0} đến ngày {1}", ((DateTime)PBDT["new_ngayphatsinh"]).AddHours(7).ToString("dd/MM/yyyy"), GioMoc.AddHours(7).ToString("dd/MM/yyyy"));
                                        dtl_tmp["new_tungay"] = ((DateTime)PBDT["new_ngayphatsinh"]).AddHours(7);
                                        dtl_tmp["new_denngay"] = GioMoc.AddHours(7);
                                        dtl_tmp["new_songay"] = (GioMoc.AddHours(7) - ((DateTime)PBDT["new_ngayphatsinh"]).AddHours(7)).Days;
                                        dtl_tmp["new_laisuat"] = (decimal)PBDT["new_laisuat"];
                                        dtl_tmp["new_tiengoc"] = new Money(sotiengoc);
                                        dtl_tmp["new_tienlai"] = new Money((sotiengoc * ((decimal)(GioMoc - (DateTime)PBDT["new_ngayphatsinh"]).Days) * (decimal)PBDT["new_laisuat"] / 3000));
                                        DoanTinhLai.Add(dtl_tmp);

                                        totallai += (sotiengoc * ((decimal)(GioMoc - (DateTime)PBDT["new_ngayphatsinh"]).Days) * (decimal)PBDT["new_laisuat"] / 3000);
                                    }
                                    else //thay đổi có bảng lãi
                                    {
                                        DateTime idx = (DateTime)PBDT["new_ngayphatsinh"];
                                        bool cothoatBL = true;

                                        List<Entity> banglai = Banglaisuat[((OptionSetValue)PBDT["new_mucdichdautu"]).Value].Where(o => ((EntityReference)o["new_vudautuapdung"]).Id == tmpVuDT).ToList();
                                        if ((DateTime)PBDT["new_ngayphatsinh"] < (DateTime)banglai[0]["new_ngayapdung"])
                                        {
                                            Entity dtl_tmp = new Entity("new_doantinhlai");
                                            dtl_tmp["new_name"] = string.Format("Lãi từ ngày {0} đến ngày {1}", ((DateTime)PBDT["new_ngayphatsinh"]).AddHours(7).ToString("dd/MM/yyyy"), ((DateTime)banglai[0]["new_ngayapdung"]).AddHours(7).ToString("dd/MM/yyyy"));
                                            dtl_tmp["new_tungay"] = ((DateTime)PBDT["new_ngayphatsinh"]).AddHours(7);
                                            dtl_tmp["new_denngay"] = ((DateTime)banglai[0]["new_ngayapdung"]).AddHours(7);
                                            dtl_tmp["new_songay"] = (((DateTime)banglai[0]["new_ngayapdung"]).AddHours(7) - ((DateTime)PBDT["new_ngayphatsinh"]).AddHours(7)).Days;
                                            dtl_tmp["new_laisuat"] = (decimal)PBDT["new_laisuat"];
                                            dtl_tmp["new_tiengoc"] = new Money(sotiengoc);
                                            dtl_tmp["new_tienlai"] = new Money((sotiengoc * ((decimal)((DateTime)banglai[0]["new_ngayapdung"] - (DateTime)PBDT["new_ngayphatsinh"]).Days) * (decimal)PBDT["new_laisuat"] / 3000));
                                            DoanTinhLai.Add(dtl_tmp);

                                            totallai += (sotiengoc * ((decimal)((DateTime)banglai[0]["new_ngayapdung"] - (DateTime)PBDT["new_ngayphatsinh"]).Days) * (decimal)PBDT["new_laisuat"] / 3000);
                                            idx = (DateTime)banglai[0]["new_ngayapdung"];
                                        }

                                        for (int i = 0; i < banglai.Count - 1; i++)
                                        {
                                            if ((DateTime)banglai[i]["new_ngayapdung"] <= idx && idx <= (DateTime)banglai[i + 1]["new_ngayapdung"] && GioMoc >= (DateTime)banglai[i + 1]["new_ngayapdung"])
                                            {
                                                if (CachTangLai == 100000000 && (decimal)banglai[i]["new_phantramlaisuat"] > defaultMucLai)
                                                {
                                                    Entity dtl_tmp = new Entity("new_doantinhlai");
                                                    dtl_tmp["new_name"] = string.Format("Lãi từ ngày {0} đến ngày {1}", idx.AddHours(7).ToString("dd/MM/yyyy"), ((DateTime)banglai[i + 1]["new_ngayapdung"]).AddHours(7).ToString("dd/MM/yyyy"));
                                                    dtl_tmp["new_tungay"] = idx.AddHours(7);
                                                    dtl_tmp["new_denngay"] = ((DateTime)banglai[i + 1]["new_ngayapdung"]).AddHours(7);
                                                    dtl_tmp["new_songay"] = (((DateTime)banglai[i + 1]["new_ngayapdung"]).AddHours(7) - idx.AddHours(7)).Days;
                                                    dtl_tmp["new_laisuat"] = defaultMucLai;
                                                    dtl_tmp["new_tiengoc"] = new Money(sotiengoc);
                                                    dtl_tmp["new_tienlai"] = new Money((sotiengoc * ((decimal)((DateTime)banglai[i + 1]["new_ngayapdung"] - idx).Days) * defaultMucLai / 3000));
                                                    DoanTinhLai.Add(dtl_tmp);

                                                    totallai += (sotiengoc * ((decimal)((DateTime)banglai[i + 1]["new_ngayapdung"] - idx).Days) * defaultMucLai / 3000);
                                                }
                                                else
                                                {
                                                    Entity dtl_tmp = new Entity("new_doantinhlai");
                                                    dtl_tmp["new_name"] = string.Format("Lãi từ ngày {0} đến ngày {1}", idx.AddHours(7).ToString("dd/MM/yyyy"), ((DateTime)banglai[i + 1]["new_ngayapdung"]).AddHours(7).ToString("dd/MM/yyyy"));
                                                    dtl_tmp["new_tungay"] = idx.AddHours(7);
                                                    dtl_tmp["new_denngay"] = ((DateTime)banglai[i + 1]["new_ngayapdung"]).AddHours(7);
                                                    dtl_tmp["new_songay"] = (((DateTime)banglai[i + 1]["new_ngayapdung"]).AddHours(7) - idx.AddHours(7)).Days;
                                                    dtl_tmp["new_laisuat"] = (decimal)banglai[i]["new_phantramlaisuat"];
                                                    dtl_tmp["new_tiengoc"] = new Money(sotiengoc);
                                                    dtl_tmp["new_tienlai"] = new Money((sotiengoc * ((decimal)((DateTime)banglai[i + 1]["new_ngayapdung"] - idx).Days) * (decimal)banglai[i]["new_phantramlaisuat"] / 3000));
                                                    DoanTinhLai.Add(dtl_tmp);

                                                    totallai += (sotiengoc * ((decimal)((DateTime)banglai[i + 1]["new_ngayapdung"] - idx).Days) * (decimal)banglai[i]["new_phantramlaisuat"] / 3000);
                                                }

                                                idx = (DateTime)banglai[i + 1]["new_ngayapdung"];
                                            }
                                            else if ((DateTime)banglai[i]["new_ngayapdung"] <= idx && idx <= (DateTime)banglai[i + 1]["new_ngayapdung"] && GioMoc < (DateTime)banglai[i + 1]["new_ngayapdung"])
                                            {
                                                cothoatBL = false;
                                                if (CachTangLai == 100000000 && (decimal)banglai[i]["new_phantramlaisuat"] > defaultMucLai)
                                                {
                                                    Entity dtl_tmp = new Entity("new_doantinhlai");
                                                    dtl_tmp["new_name"] = string.Format("Lãi từ ngày {0} đến ngày {1}", idx.AddHours(7).ToString("dd/MM/yyyy"), ((DateTime)banglai[i + 1]["new_ngayapdung"]).AddHours(7).ToString("dd/MM/yyyy"));
                                                    dtl_tmp["new_tungay"] = idx.AddHours(7);
                                                    dtl_tmp["new_denngay"] = ((DateTime)banglai[i + 1]["new_ngayapdung"]).AddHours(7);
                                                    dtl_tmp["new_songay"] = (((DateTime)banglai[i + 1]["new_ngayapdung"]).AddHours(7) - idx.AddHours(7)).Days;
                                                    dtl_tmp["new_laisuat"] = defaultMucLai;
                                                    dtl_tmp["new_tiengoc"] = new Money(sotiengoc);
                                                    dtl_tmp["new_tienlai"] = new Money((sotiengoc * ((decimal)((DateTime)banglai[i + 1]["new_ngayapdung"] - idx).Days) * defaultMucLai / 3000));
                                                    DoanTinhLai.Add(dtl_tmp);

                                                    totallai += (sotiengoc * ((decimal)((DateTime)banglai[i + 1]["new_ngayapdung"] - idx).Days) * defaultMucLai / 3000);
                                                }
                                                else
                                                {
                                                    Entity dtl_tmp = new Entity("new_doantinhlai");
                                                    dtl_tmp["new_name"] = string.Format("Lãi từ ngày {0} đến ngày {1}", idx.AddHours(7).ToString("dd/MM/yyyy"), GioMoc.AddHours(7).ToString("dd/MM/yyyy"));
                                                    dtl_tmp["new_tungay"] = idx.AddHours(7);
                                                    dtl_tmp["new_denngay"] = GioMoc.AddHours(7);
                                                    dtl_tmp["new_songay"] = (GioMoc.AddHours(7) - idx.AddHours(7)).Days;
                                                    dtl_tmp["new_laisuat"] = (decimal)banglai[i]["new_phantramlaisuat"];
                                                    dtl_tmp["new_tiengoc"] = new Money(sotiengoc);
                                                    dtl_tmp["new_tienlai"] = new Money((sotiengoc * ((decimal)(GioMoc - idx).Days) * (decimal)banglai[i]["new_phantramlaisuat"] / 3000));
                                                    DoanTinhLai.Add(dtl_tmp);

                                                    totallai += (sotiengoc * ((decimal)(GioMoc - idx).Days) * (decimal)banglai[i]["new_phantramlaisuat"] / 3000);
                                                }
                                                break;
                                            }
                                        }
                                        if (cothoatBL)
                                        {
                                            if (CachTangLai == 100000000 && (decimal)banglai[banglai.Count - 1]["new_phantramlaisuat"] > defaultMucLai)
                                            {
                                                Entity dtl_tmp = new Entity("new_doantinhlai");
                                                dtl_tmp["new_name"] = string.Format("Lãi từ ngày {0} đến ngày {1}", idx.AddHours(7).ToString("dd/MM/yyyy"), GioMoc.AddHours(7).ToString("dd/MM/yyyy"));
                                                dtl_tmp["new_tungay"] = idx.AddHours(7);
                                                dtl_tmp["new_denngay"] = GioMoc.AddHours(7);
                                                dtl_tmp["new_songay"] = (GioMoc.AddHours(7) - idx.AddHours(7)).Days;
                                                dtl_tmp["new_laisuat"] = defaultMucLai;
                                                dtl_tmp["new_tiengoc"] = new Money(sotiengoc);
                                                dtl_tmp["new_tienlai"] = new Money((sotiengoc * ((decimal)(GioMoc - idx).Days) * defaultMucLai / 3000));
                                                DoanTinhLai.Add(dtl_tmp);

                                                totallai += (sotiengoc * ((decimal)(GioMoc - idx).Days) * defaultMucLai / 3000);
                                            }
                                            else
                                            {
                                                Entity dtl_tmp = new Entity("new_doantinhlai");
                                                dtl_tmp["new_name"] = string.Format("Lãi từ ngày {0} đến ngày {1}", idx.AddHours(7).ToString("dd/MM/yyyy"), GioMoc.AddHours(7).ToString("dd/MM/yyyy"));
                                                dtl_tmp["new_tungay"] = idx.AddHours(7);
                                                dtl_tmp["new_denngay"] = GioMoc.AddHours(7);
                                                dtl_tmp["new_songay"] = (GioMoc.AddHours(7) - idx.AddHours(7)).Days;
                                                dtl_tmp["new_laisuat"] = (decimal)banglai[banglai.Count - 1]["new_phantramlaisuat"];
                                                dtl_tmp["new_tiengoc"] = new Money(sotiengoc);
                                                dtl_tmp["new_tienlai"] = new Money((sotiengoc * ((decimal)(GioMoc - idx).Days) * (decimal)banglai[banglai.Count - 1]["new_phantramlaisuat"] / 3000));
                                                DoanTinhLai.Add(dtl_tmp);

                                                totallai += (sotiengoc * ((decimal)(GioMoc - idx).Days) * (decimal)banglai[banglai.Count - 1]["new_phantramlaisuat"] / 3000);
                                            }
                                        }
                                    }

                                    if (HinhThucTra == 100000001) //Trả tổng tiền
                                    {
                                        if ((sotiengoc + totallai + sumgoc + sumlai) <= ThuTongTien) //ghi nhận lãi cho phiếu PBDT
                                        {
                                            result["new_tienvay"] = new Money(sotiengoc);
                                            result["new_ngayvay"] = (DateTime)PBDT["new_ngayphatsinh"];
                                            result["new_ngaytra"] = GioMoc;
                                            //phieutinhlai["new_laisuat"] = (decimal)PBDT["new_laisuat"];
                                            result["new_tienlai"] = new Money(totallai);
                                            result["new_songay"] = (int)(GioMoc - (DateTime)PBDT["new_ngayphatsinh"]).Days;
                                        }
                                        else //Tam xuất lại ra số tiền
                                        {
                                            decimal sotiengocmoi = sotiengoc * (ThuTongTien - sumgoc - sumlai) / (sotiengoc + totallai);
                                            decimal totallaimoi = ThuTongTien - sumgoc - sumlai - sotiengocmoi;
                                            //decimal totallaimoi = totallai * Tiengoc / (sotiengoc + totallai);
                                            foreach (Entity lt in DoanTinhLai)
                                            {
                                                lt["new_tiengoc"] = new Money(sotiengocmoi);
                                                lt["new_tienlai"] = new Money(((Money)lt["new_tienlai"]).Value * (ThuTongTien - sumgoc - sumlai) / (sotiengoc + totallai));
                                            }

                                            result["new_tienvay"] = new Money(sotiengocmoi);
                                            result["new_ngayvay"] = (DateTime)PBDT["new_ngayphatsinh"];
                                            result["new_ngaytra"] = GioMoc;
                                            //phieutinhlai["new_laisuat"] = (decimal)PBDT["new_laisuat"];
                                            result["new_tienlai"] = new Money(totallaimoi);
                                            result["new_songay"] = (int)(GioMoc - (DateTime)PBDT["new_ngayphatsinh"]).Days;
                                            sotiengoc = sotiengocmoi;
                                            totallai = totallaimoi;
                                        }
                                    }
                                    else
                                    {
                                        result["new_tienvay"] = new Money(sotiengoc);
                                        result["new_ngayvay"] = (DateTime)PBDT["new_ngayphatsinh"];
                                        result["new_ngaytra"] = GioMoc;
                                        //phieutinhlai["new_laisuat"] = (decimal)PBDT["new_laisuat"];
                                        result["new_tienlai"] = new Money(totallai);
                                        result["new_songay"] = (int)(GioMoc - (DateTime)PBDT["new_ngayphatsinh"]).Days;
                                    }

                                    break;
                                }
                            case 100000001: // lai tren tong tien
                                {
                                    //chưa làm
                                    break;
                                }
                        }

                        //Create mọi thứ

                        foreach (Entity entry in DoanTinhLai)
                        {
                            entry["new_phieudenghithuno"] = new EntityReference("new_phieudenghithuno", PDNThuNo.Id);
                            entry["new_phieutinhlai"] = new EntityReference("new_phieutinhlai", result.Id);
                            service.Create(entry);
                        }
                        service.Update(result);

                        Entity tmp = new Entity("new_phieudenghithuno");
                        tmp.Id = PDNThuNo.Id;
                        if (HinhThucTra == 100000000)
                            tmp["new_tongtienthu"] = new Money(sumgoc + sumlai + totallai + sotiengoc);
                        else
                            tmp["new_thunogoc"] = new Money(sumgoc + sotiengoc);
                        tmp["new_thulai"] = new Money(sumlai + totallai);
                        service.Update(tmp);
                    }

                }
            }

        }

        public void GetSumPDNThuNo(Guid PDNThuNoId, Guid PTLId, ref decimal tiengoc, ref decimal tienlai)
        {
            QueryExpression qe = new QueryExpression("new_phieutinhlai");
            qe.ColumnSet = new ColumnSet("new_tienvay", "new_tienlai");
            qe.Criteria.Conditions.Add(new ConditionExpression("new_phieudenghithuno", ConditionOperator.Equal, PDNThuNoId));
            qe.Criteria.Conditions.Add(new ConditionExpression("new_phieutinhlaiid", ConditionOperator.NotEqual, PTLId));

            EntityCollection result = service.RetrieveMultiple(qe);
            tiengoc = result.Entities.Sum(o => o.Contains("new_tienvay") ? ((Money)o["new_tienvay"]).Value : (decimal)0);
            tienlai = result.Entities.Sum(o => o.Contains("new_tienlai") ? ((Money)o["new_tienlai"]).Value : (decimal)0);
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

        public void loadBangLai()
        {
            QueryExpression qe = new QueryExpression("new_banglaisuatthaydoi");
            qe.ColumnSet = new ColumnSet("new_mucdichdautu", "new_ngayapdung", "new_phantramlaisuat", "new_vudautuapdung");
            qe.Orders.Add(new OrderExpression("new_mucdichdautu", OrderType.Ascending));

            int queryCount = 5000;
            int pageNumber = 1;
            qe.PageInfo = new PagingInfo();
            qe.PageInfo.Count = queryCount;
            qe.PageInfo.PageNumber = pageNumber;
            qe.PageInfo.PagingCookie = null;

            int pre = -1;
            List<Entity> dsLai = new List<Entity>();

            while (true)
            {
                EntityCollection results = service.RetrieveMultiple(qe);
                foreach (Entity a in results.Entities)
                {
                    if (pre == -1)
                        pre = ((OptionSetValue)a["new_mucdichdautu"]).Value;

                    if (pre != ((OptionSetValue)a["new_mucdichdautu"]).Value)
                    {
                        Banglaisuat.Add(pre, dsLai.OrderBy(o => (DateTime)o["new_ngayapdung"]).ToList());
                        pre = ((OptionSetValue)a["new_mucdichdautu"]).Value;
                        dsLai = new List<Entity>();
                        dsLai.Add(a);
                    }
                    else dsLai.Add(a);
                }

                if (results.MoreRecords)
                {
                    qe.PageInfo.PageNumber++;
                    qe.PageInfo.PagingCookie = results.PagingCookie;
                }
                else
                {
                    if (pre != -1)
                        Banglaisuat.Add(pre, dsLai.OrderBy(o => (DateTime)o["new_ngayapdung"]).ToList());
                    break;
                }
            }
        }
    }
}
