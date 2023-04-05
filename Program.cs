// See https://aka.ms/new-console-template for more information
//Console.WriteLine("Hello, World!");



//classlibraries/namespaces
using MQTTnet;
using MQTTnet.Client;
using System;
using System.ComponentModel;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Linq;
using System.Net.NetworkInformation;
using System.Text;
using System.Threading.Tasks;
using System.Text.Json;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.DependencyInjection;

//declaring namespace
namespace CertSpecUNS
{
    public class Program
    {
        public static void Main(string[] args)
        {
            CreateHostBuilder(args).Build().Run();
        }
        public static IHostBuilder CreateHostBuilder(string[] args) => Host.CreateDefaultBuilder(args).ConfigureServices((hostContext, services) =>
        {
            services.AddHostedService<Worker>();
        });
        
        public class Worker : BackgroundService
        {


            private readonly ILogger<Worker> _logger;

            public Worker(ILogger<Worker> logger)
            {
                _logger = logger;
            }

            protected override async Task ExecuteAsync(CancellationToken stoppingToken)
            {
                while (!stoppingToken.IsCancellationRequested)
                {
                    _logger.LogInformation("Worker running at: {time}", DateTimeOffset.Now);
                    await leeDatosAsync();
                    await Task.Delay(1000, stoppingToken);
                }
            }

            public async Task leeDatosAsync()
            {
                DataSet1 sp = new DataSet1();
                DataSet1TableAdapters.sp_MaterialCertToMQTTTableAdapter uns = new DataSet1TableAdapters.sp_MaterialCertToMQTTTableAdapter();
                
                List<data> data = new List<data>();
                uns.Fill(sp.sp_MaterialCertToMQTT);
                if (sp.sp_MaterialCertToMQTT.Rows.Count > 0)
                {
                    for (int i = 0; i < sp.sp_MaterialCertToMQTT.Rows.Count; i++)
                    {
                        data a = new data();
                        a.matnr = sp.sp_MaterialCertToMQTT.Rows[i][0].ToString();
                        a.coiln = sp.sp_MaterialCertToMQTT.Rows[i][1].ToString();
                        a.scoiln = sp.sp_MaterialCertToMQTT.Rows[i][2].ToString();
                        a.oem = sp.sp_MaterialCertToMQTT.Rows[i][3].ToString();
                        a.spec_grade = sp.sp_MaterialCertToMQTT.Rows[i][4].ToString();
                        a.grade_name = sp.sp_MaterialCertToMQTT.Rows[i][5].ToString();
                        a.datin = sp.sp_MaterialCertToMQTT.Rows[i][6].ToString();
                        a.name1 = sp.sp_MaterialCertToMQTT.Rows[i][7].ToString();
                        a.width = Convert.ToDecimal(sp.sp_MaterialCertToMQTT.Rows[i][8].ToString());
                        a.width_uom = "mm";
                        a.thickness = Convert.ToDecimal(sp.sp_MaterialCertToMQTT.Rows[i][10].ToString());
                        a.thickness_uom = "mm";
                        a.weight = Convert.ToDecimal(sp.sp_MaterialCertToMQTT.Rows[i][12].ToString());
                        a.weight_uom = "kg";
                        a.yield_strength = Convert.ToDecimal(sp.sp_MaterialCertToMQTT.Rows[i][14].ToString());
                        a.yield_strength_uom = "Mpa";
                        a.tensile_strength = Convert.ToDecimal(sp.sp_MaterialCertToMQTT.Rows[i][16].ToString());
                        a.tensile_strength_uom = "Mpa";
                        a.elongation = Convert.ToDecimal(sp.sp_MaterialCertToMQTT.Rows[i][18].ToString());
                        a.elongation_uom = "%";
                        a.rvalue = Convert.ToDecimal(sp.sp_MaterialCertToMQTT.Rows[i][19].ToString());
                        a.rvalue_uom = "mm";
                        a.nvalue = Convert.ToDecimal(sp.sp_MaterialCertToMQTT.Rows[i][21].ToString());
                        a.nvalue_uom = "%";
                        a.elongation_fracture = Convert.ToDecimal(sp.sp_MaterialCertToMQTT.Rows[i][23].ToString());
                        a.uniform_elongation = Convert.ToDecimal(sp.sp_MaterialCertToMQTT.Rows[i][24].ToString());
                        a.UID = sp.sp_MaterialCertToMQTT.Rows[i][25].ToString();
                        //status.sp_WorkerService_status(a.coiln, true);
                        data.Add(a);
                        
                    }
                    await FirstProcess("m/kmk/receiving/materialcert", data);
                }

                //DataSet1TableAdapters.sp_MaterialSpecToMQTTTableAdapter spec = new DataSet1TableAdapters.sp_MaterialSpecToMQTTTableAdapter();
                ////DataSet1TableAdapters.QueriesTableAdapter status = new DataSet1TableAdapters.QueriesTableAdapter();

                //List<data3> data3 = new List<data3>();
                ////List<data2> data2 = new List<data2>();
                //spec.Fill(sp.sp_MaterialSpecToMQTT);
                //if (sp.sp_MaterialSpecToMQTT.Rows.Count > 0)
                //{
                //    for (int i = 0; i < sp.sp_MaterialSpecToMQTT.Rows.Count; i++)
                //    {
                //        //data2 p = new data2();
                //        data3 a = new data3();
                //        a.matnr = sp.sp_MaterialSpecToMQTT.Rows[i][0].ToString();
                //        List<propertys> properties = new List<propertys>();
                //        propertys c = new propertys();

                //        a.coiln = sp.sp_MaterialSpecToMQTT.Rows[i][1].ToString();
                //        c.property = "width";

                //        //converts floating-point number to a string that returns as a string or combination
                //        try
                //        {
                //            c.max = Convert.ToDouble(sp.sp_MaterialSpecToMQTT.Rows[i][2].ToString());
                //        }
                //        catch (Exception)
                //        {
                //            c.max = null;
                //        }
                //        try
                //        {
                //            c.min = Convert.ToDouble(sp.sp_MaterialSpecToMQTT.Rows[i][3].ToString());
                //        }
                //        catch (Exception)
                //        {
                //            c.min = null;
                //        }
                //        c.uom = "mm";
                //        properties.Add(c);

                //        propertys t = new propertys();
                //        t.property = "thickness";
                //        t.uom = "mm";

                //        try
                //        {
                //            t.max = Convert.ToDouble(sp.sp_MaterialSpecToMQTT.Rows[i][4].ToString());
                //        }
                //        catch (Exception)
                //        {
                //            t.max = null;
                //        }
                //        try
                //        {
                //            t.min = Convert.ToDouble(sp.sp_MaterialSpecToMQTT.Rows[i][5].ToString());
                //        }
                //        catch (Exception)
                //        {
                //            t.min = null;
                //        }
                //        properties.Add(t);

                //        propertys w = new propertys();
                //        w.property = "weight";
                //        w.uom = "kg";
                //        try
                //        {
                //            w.max = Convert.ToDouble(sp.sp_MaterialSpecToMQTT.Rows[i][6].ToString());
                //        }
                //        catch (Exception)
                //        {
                //            w.max = null;
                //        }
                //        try
                //        {
                //            w.min = Convert.ToDouble(sp.sp_MaterialSpecToMQTT.Rows[i][7].ToString());
                //        }
                //        catch (Exception)
                //        {
                //            w.min = null;
                //        }
                //        properties.Add(w);

                //        propertys ys = new propertys();
                //        ys.property = "yeild_strength";
                //        ys.uom = "Mpa";
                //        try
                //        {
                //            ys.max = Convert.ToDouble(sp.sp_MaterialSpecToMQTT.Rows[i][8].ToString());
                //        }
                //        catch
                //        {
                //            ys.max = null;
                //        }
                //        try
                //        {
                //            ys.min = Convert.ToDouble(sp.sp_MaterialSpecToMQTT.Rows[i][9].ToString());
                //        }
                //        catch (Exception)
                //        {
                //            ys.min = null;
                //        }
                //        properties.Add(ys);

                //        propertys ts = new propertys();
                //        ts.property = "tensile_strength";
                //        ts.uom = "Mpa";
                //        try
                //        {
                //            ts.max = Convert.ToDouble(sp.sp_MaterialSpecToMQTT.Rows[i][10].ToString());
                //        }
                //        catch (Exception)
                //        {
                //            ts.max = null;
                //        }
                //        try
                //        {
                //            ts.min = Convert.ToDouble(sp.sp_MaterialSpecToMQTT.Rows[i][11].ToString());
                //        }
                //        catch (Exception)
                //        {
                //            ts.min = null;
                //        }
                //        properties.Add(ts);

                //        propertys e = new propertys();
                //        e.property = "elongation";
                //        e.uom = "%";
                //        try
                //        {
                //            e.max = Convert.ToDouble(sp.sp_MaterialSpecToMQTT.Rows[i][12].ToString());
                //        }
                //        catch (Exception)
                //        {
                //            e.max = null;
                //        }
                //        try
                //        {
                //            e.min = Convert.ToDouble(sp.sp_MaterialSpecToMQTT.Rows[i][13].ToString());
                //        }
                //        catch (Exception)
                //        {
                //            e.min = null;
                //        }
                //        properties.Add(e);

                //        propertys nv = new propertys();
                //        nv.property = "n value";
                //        nv.uom = "%";
                //        try
                //        {
                //            nv.max = Convert.ToDouble(sp.sp_MaterialSpecToMQTT.Rows[i][14].ToString());
                //        }
                //        catch (Exception)
                //        {
                //            nv.max = null;
                //        }
                //        try
                //        {
                //            nv.min = Convert.ToDouble(sp.sp_MaterialSpecToMQTT.Rows[i][15].ToString());
                //        }
                //        catch (Exception)
                //        {
                //            nv.min = null;
                //        }
                //        properties.Add(nv);

                //        propertys rv = new propertys();
                //        rv.property = "r value";
                //        rv.uom = "mm";
                //        try
                //        {
                //            rv.max = Convert.ToDouble(sp.sp_MaterialSpecToMQTT.Rows[i][16].ToString());
                //        }
                //        catch (Exception)
                //        {
                //            rv.max = null;
                //        }
                //        try
                //        {
                //            rv.min = Convert.ToDouble(sp.sp_MaterialSpecToMQTT.Rows[i][17].ToString());
                //        }
                //        catch (Exception)
                //        {
                //            rv.min = null;
                //        }
                //        properties.Add(rv);

                //        a.mat_props_minmax = properties;
                //        data3.Add(a);
                //        //status.WorkerService_status(a.coiln, true);
                //    }
                //    await SecondProcess("m/kml/receiving/materialcert", data3);
                //}

            }
            public static async Task FirstProcess(string topic, List<data> data)
            {
                var mqttFactory = new MqttFactory();

                var mensaje = DateTime.UtcNow;
                json js = new json();
                js.Timestamp = mensaje.ToString("s");
                js.MaterialP = data;

                using (var mqttClient = mqttFactory.CreateMqttClient())
                {
                    var mqttClientOptions = new MqttClientOptionsBuilder()
                        .WithCredentials("kmk_863_MaterialCert", "WhyQu@l1ty2023").WithConnectionUri("mqtt://kmkuns01srv:1883")
                        .Build();

                    await mqttClient.ConnectAsync(mqttClientOptions, CancellationToken.None);

                    var applicationMessage = new MqttApplicationMessageBuilder()
                        .WithTopic(topic)
                        .WithPayload(JsonSerializer.Serialize(js))
                        .Build();

                    await mqttClient.PublishAsync(applicationMessage, CancellationToken.None);

                    Console.WriteLine("MQTT application message is published to the UNS.");
                }
            }

            //public async Task SecondProcess(string topic, List<data3> data)
            //{
            //    var mqttFactory = new MqttFactory();

            //    var mensaje = DateTime.UtcNow;
            //    json2 js = new json2();
            //    js.Timestamp = mensaje.ToString("s");
            //    js.minmax = data;
            //    var json2 = JsonSerializer.Serialize(js);

            //    //creates a new mqtt server
            //    using (var mqttClient = mqttFactory.CreateMqttClient())
            //    {
            //        //creates connection options for the mqtt broker
            //        var mqttClientOptions = new MqttClientOptionsBuilder()
            //            .WithCredentials("kmk_863_MaterialCert", "WhyQu@l1ty2023").WithConnectionUri("mqtt://kmkuns01srv:1883")
            //            .Build();

            //        await mqttClient.ConnectAsync(mqttClientOptions, CancellationToken.None);

            //        //the mqtt broker publishes the messages received from the SQL database
            //        var applicationMessage = new MqttApplicationMessageBuilder()
            //            .WithTopic(topic)
            //            .WithPayload(json2)//shows how to compose the information 
            //            .Build();

            //        await mqttClient.PublishAsync(applicationMessage, CancellationToken.None);

            //        Console.WriteLine("MQTT application message is published to the UNS.");

            //    }
            //}
            
        }
    }
    //mqtt connection: add the url with creds
    //declarations of lists
    public class json
    {
        public string? Timestamp { get; set; }
        public List<data> MaterialP { get; set; }
        
    }

    //public class json2
    //{
    //    public string Timestamp { get; set; }
    //    public List<data3> minmax { get; set; }
    //}

    //public class propertys
    //{
    //    public string property { get; set; }
    //    public string uom { get; set; }
    //    public double? min { get; set; }
    //    public double? max { get; set; }
    //}
    public class data
    {
        public string matnr { get; set; }
        public string coiln { get; set; }
        public string? oem { get; set; }
        public string? spec_grade { get; set; }
        public string? grade_name { get; set; }
        public string datin { get; set; }
        public string name1 { get; set; }
        public decimal width { get; set; }
        public string width_uom { get; set; }
        public decimal thickness { get; set; }
        public string thickness_uom { get; set; }
        public decimal weight { get; set; }
        public string weight_uom { get; set; }
        public decimal yield_strength { get; set; }
        public string yield_strength_uom { get; set; }
        public decimal tensile_strength { get; set; }
        public string tensile_strength_uom { get; set; }
        public decimal elongation { get; set; }
        public string elongation_uom { get; set; }
        public decimal nvalue { get; set; }
        public string nvalue_uom { get; set; }
        public decimal rvalue { get; set; }
        public string rvalue_uom { get; set; }
        public string? scoiln { get; set; }
        public decimal elongation_fracture { get; set; }
        public decimal uniform_elongation { get; set; }
        public string UID { get; set; }
    }

    //public class data3
    //{
    //    public string coiln { get; set; }
    //    public string matnr { get; set; }
    //    public List<propertys>? mat_props_minmax { get; set; }
    //}
}