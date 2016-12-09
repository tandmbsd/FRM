using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Web.Script.Serialization;
using System.Net;
using Microsoft.Xrm.Sdk.Messages;

namespace Action_GetPolygonData
{
    public class Action_GetPolygonData : IPlugin
    {
        IOrganizationService service = null;
        IOrganizationServiceFactory factory = null;
        void IPlugin.Execute(IServiceProvider serviceProvider)
        {
            IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
            service = factory.CreateOrganizationService(context.UserId);

            GeoData data = new GeoData();
            data.type = "FeatureCollection";
            List<GeoItem> features = new List<GeoItem>();
            try
            {
                int fetchCount = 5000;
                int pageNumber = 1;
                string pagingCookie = null;

                while (true)
                {
                    #region fetch
                    string fetch = string.Format(@"<fetch version='1.0' mapping='logical' paging-cookie='{0}' page='{1}' count='{2}'>" +
                      "<entity name='new_polygon'>" +
                        "<attribute name='new_thuadat' />" +
                        "<attribute name='new_long' />" +
                        "<attribute name='new_lat' />" +
                        "<attribute name='new_polygonid' />" +
                        "<order attribute='new_thuadat' descending='false' />" +
                        "<order attribute='new_rowid' descending='false' />" +
                        "<filter type='and'>" +
                          "<condition attribute='statecode' operator='eq' value='0' />" +
                        "</filter>" +
                        "<link-entity name='new_thuadat' from='new_thuadatid' to='new_thuadat' alias='ae'>" +
                         "<attribute name='new_nhomdat'/>" +
                         "<attribute name='new_nguonnuoc'/>" + 
                          "<link-entity name='new_thuadatcanhtac' from='new_thuadat' to='new_thuadatid' alias='af'>" +
                            "<attribute name='new_tuoimia'/>" +
                            "<attribute name='new_loaisohuudat'/>" +
                            "<attribute name='new_vutrong'/>" +
                            "<attribute name='new_loaigocmia'/>" +
                            "<attribute name='new_mucdichsanxuatmia'/>" +
                            "<attribute name='new_ngaythuhoachdukien'/>" +
                            "<attribute name='new_dientichconlai'/>" +
                            "<attribute name='new_khachhang'/>" +
                            "<attribute name='new_khachhangdoanhnghiep'/>" +
                            "<attribute name='new_ngaytrong'/>" +
                            "<attribute name='new_miachay'/>" +
                            "<attribute name='new_giongtrongthucte'/>" +
                            "<attribute name='new_loaigocmia'/>" +
                            "<attribute name='new_luugoc'/>" +
                            "<attribute name='new_tram'/>" +
                            "<attribute name='new_thuadatcanhtacid'/>" +
                            "<link-entity name='new_hopdongdautumia' from='new_hopdongdautumiaid' to='new_hopdongdautumia' alias='ag'>" +
                              "<link-entity name='new_vudautu' from='new_vudautuid' to='new_vudautu' alias='ah'>" +
                                "<filter type='and'>" +
                                  "<condition attribute='new_danghoatdong' operator='eq' value='1' />" +
                                "</filter>" +
                              "</link-entity>" +
                            "</link-entity>" +
                          "</link-entity>" +
                        "</link-entity>" +
                      "</entity>" +
                    "</fetch>", WebUtility.HtmlEncode(pagingCookie), pageNumber, fetchCount);
                    EntityCollection result = service.RetrieveMultiple(new FetchExpression(fetch));

                    #endregion

                    Guid tdid = Guid.Empty;
                    GeoItem item = null;
                    List<decimal[][]> daf = new List<decimal[][]>();
                    List<decimal[]> cord = new List<decimal[]>();

                    foreach (Entity a in result.Entities)
                    {
                        Guid td = (a.Contains("new_thuadat") ? ((EntityReference)a["new_thuadat"]).Id : Guid.Empty);
                        if (td != tdid)
                        {
                            if (item != null)
                            {
                                cord.Add(cord[0]);
                                daf.Add(cord.ToArray());
                                item.geometry.coordinates = daf.ToArray();
                                features.Add(item);
                            }
                            item = new GeoItem();
                            daf = new List<decimal[][]>();
                            cord = new List<decimal[]>();
                            tdid = td;
                            string KH = "";
                            if (a.Contains("af.new_khachhang"))
                            {
                                if (((AliasedValue)a["af.new_khachhang"]).Value != null)
                                    KH = ((EntityReference)((AliasedValue)a["af.new_khachhang"]).Value).Name;
                            }
                            if (KH == "" && a.Contains("af.new_khachhangdoanhnghiep"))
                            {
                                if (((AliasedValue)a["af.new_khachhangdoanhnghiep"]).Value != null)
                                    KH = ((EntityReference)((AliasedValue)a["af.new_khachhangdoanhnghiep"]).Value).Name;
                            }

                            int x = (int)(DateTime.Now - (a.Contains("af.new_ngaytrong") ? (((AliasedValue)a["af.new_ngaytrong"]).Value != null ? (DateTime)((AliasedValue)a["af.new_ngaytrong"]).Value : DateTime.Now) : DateTime.Now)).TotalDays;
                            bool miachay = (a.Contains("af.new_miachay") ? (((AliasedValue)a["af.new_miachay"]).Value != null ? (bool)((AliasedValue)a["af.new_miachay"]).Value : false) : false);

                            int nhomdat = (a.Contains("ae.new_nhomdat") ? (((AliasedValue)a["ae.new_nhomdat"]).Value != null ? ((OptionSetValue)((AliasedValue)a["ae.new_nhomdat"]).Value).Value : 100000000) : 100000000);

                            int loaigocmia = (a.Contains("af.new_loaigocmia") ? (((AliasedValue)a["af.new_loaigocmia"]).Value != null ? ((OptionSetValue)((AliasedValue)a["af.new_loaigocmia"]).Value).Value : 100000000) : 100000000);
                            int luugoc = (a.Contains("af.new_luugoc") ? (((AliasedValue)a["af.new_luugoc"]).Value != null ? ((OptionSetValue)((AliasedValue)a["af.new_luugoc"]).Value).Value : 100000000) : 100000000);
                            int loaisohuu = (a.Contains("af.new_loaisohuudat") ? (((AliasedValue)a["af.new_loaisohuudat"]).Value != null ? ((OptionSetValue)((AliasedValue)a["af.new_loaisohuudat"]).Value).Value : 100000000) : 100000000);
                            bool nguonnuoc = (a.Contains("ae.new_nguonnuoc") ? (((AliasedValue)a["ae.new_nguonnuoc"]).Value != null ? (bool)((AliasedValue)a["ae.new_nguonnuoc"]).Value : false) : false);

                            item.type = "Feature";
                            //properties
                            item.properties = new GeoProperties()
                            {
                                //id = (a.Contains("af.new_thuadatcanhtacid") ? (((AliasedValue)a["af.new_thuadatcanhtacid"]).Value != null ? ((AliasedValue)a["af.new_thuadatcanhtacid"]).Value.ToString().Replace("{", "").Replace("}","") : "") : ""),
                                a = (a.Contains("af.new_tuoimia") ? (((AliasedValue)a["af.new_tuoimia"]).Value != null ? (bool)((AliasedValue)a["af.new_tuoimia"]).Value : false) : false),
                                b = loaisohuu,
                                c = (a.Contains("af.new_vutrong") ? (((AliasedValue)a["af.new_vutrong"]).Value != null ? ((OptionSetValue)((AliasedValue)a["af.new_vutrong"]).Value).Value : 100000000) : 100000000),
                                d = (a.Contains("af.new_loaigocmia") ? (((AliasedValue)a["af.new_loaigocmia"]).Value != null ? ((OptionSetValue)((AliasedValue)a["af.new_loaigocmia"]).Value).Value : 100000000) : 100000000),
                                e = (a.Contains("af.new_mucdichsanxuatmia") ? (((AliasedValue)a["af.new_mucdichsanxuatmia"]).Value != null ? ((OptionSetValue)((AliasedValue)a["af.new_mucdichsanxuatmia"]).Value).Value : 100000000) : 100000000),
                                f = x,
                                g = miachay,
                                h = (a.Contains("af.new_dientichconlai") ? (((AliasedValue)a["af.new_dientichconlai"]).Value != null ? (decimal)((AliasedValue)a["af.new_dientichconlai"]).Value : (decimal)0) : (decimal)0),
                                k = KH,
                                l = (a.Contains("new_thuadat") ? ((EntityReference)a["new_thuadat"]).Name : ""),
                                m = (nhomdat == 100000000 ? "Đất cao" : (nhomdat == 100000001 ? "Đất thấp" : "Đất triền")),//nhom dat
                                n = (a.Contains("af.new_giongtrongthucte") ? (((AliasedValue)a["af.new_giongtrongthucte"]).Value != null ? ((EntityReference)((AliasedValue)a["af.new_giongtrongthucte"]).Value).Name : "") : ""),
                                o = (loaigocmia == 100000000 ? "Mía tơ" : "Mía gốc " + (luugoc - 99999999).ToString()),
                                p = (loaisohuu == 100000000 ? "Đất nhà" : "Đất thuê"),
                                q = (a.Contains("af.new_tram") ? (((AliasedValue)a["af.new_tram"]).Value != null ? ((EntityReference)((AliasedValue)a["af.new_tram"]).Value).Name : "") : ""),
                                r = (nguonnuoc ? "Có" : "Không"),
                                color = GetColor(x, miachay)
                            };

                            //geometry
                            item.geometry = new Geometry();
                            item.geometry.type = "Polygon";
                            cord.Add(new decimal[] { (a.Contains("new_long") ? (decimal)a["new_long"]: (decimal)0 ),
                                    (a.Contains("new_lat") ? (decimal)a["new_lat"]: (decimal)0 )
                                });
                        }
                        else
                        {
                            cord.Add(new decimal[] { (a.Contains("new_long") ? (decimal)a["new_long"]: (decimal)0 ),
                                    (a.Contains("new_lat") ? (decimal)a["new_lat"]: (decimal)0 )
                                });
                        }
                    }

                    if (result.MoreRecords)
                    {
                        pageNumber++;
                        pagingCookie = result.PagingCookie;
                    }
                    else
                    {
                        break;
                    }
                }
            }
            catch (Exception ex)
            {
                //context.OutputParameters["Return"] = ex.Message;
            }

            data.features = features.ToArray();
            var json = new JavaScriptSerializer().Serialize(data);

            context.OutputParameters["Return"] = json;
        }

        static string GetColor(int day, bool chay)
        {
            string[] color = new string[] { "gray", "#00FFB3", "#00FF62", "#B7FF00", "#FFEE00", "#FFA200", "#FF1900", "black" };
            if (chay)
                return color[7];
            if (day == 0)
                return color[0];
            if (day <= 60)
                return color[1];
            else if (day > 60 && day <= 120)
                return color[2];
            else if (day > 120 && day <= 275)
                return color[3];
            else if (day > 275 && day <= 360)
                return color[4];
            else if (day > 360 && day <= 400)
                return color[5];
            else 
                return color[6];
        }

    }
}
