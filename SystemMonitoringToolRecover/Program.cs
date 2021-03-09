using SystemMonitoring.AlertRecoveryTool.Model;

namespace SystemMonitoring.AlertRecoveryTool
{
    using System;
    using Polly.Timeout;
    using System.Collections.Generic;
    using System.Configuration;
    using System.Linq;
    using System.Reactive.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using System.Threading.Tasks.Dataflow;
    using Azure;
    using Azure.Search.Documents.Indexes;
    using Azure.Search.Documents.Models;

    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Blob;

    using Newtonsoft.Json;
    using Polly;

    public enum StatisticType
    {
        BufferedAlertsByNow,
        PushedAlertsToAzureByNow,
        BlobsDownloadedByNow
    }

    public class Program
    {
        private static CloudBlobContainer _container;
        private static string _pathPrefix;

        private static DateTimeOffset _minTimePeriod;
        private static DateTimeOffset _maxTimePeriod;
        private static List<string> _blobPathsSource;

        private static BlobType _blobType;
        private static ActiveAlertType _alertType;
        private static string _alertName;
        private static AlertExpirationPolicy _alertExpirationPolicy;
        private static long _alertExpirationShiftingInDays;

        private static string _targetSearchServiceName;
        private static string _targetAdminKey;
        private static string _targetIndexName;
        private static long _timeOutInSeconds;
        private static int _alertsBatchSize;
        private static SearchIndexClient _targetIndexClient;

        private static int _bufferedByNow = 0;
        private static int _pushedToAzureByNow = 0;
        private static int _blobsDownloadedByNow = 0;

        private static bool _forcedTriggerBatchRequired = true;

        private static readonly BatchBlock<ActiveAlertAzureSearchModel> _batchBlock = new BatchBlock<ActiveAlertAzureSearchModel>(300);

        private static ActionBlock<ActiveAlertAzureSearchModel[]> _actionBlock;

        private static CancellationTokenSource _cancellationSource;
        private static Guid? _alertTenantId;

        public static async Task Main(string[] args)
        {
            try
            {
                ConfigurationSetup();
                ParseBlobPaths();
                await RecoverBlobs().ConfigureAwait(false);

                Console.WriteLine($"{Environment.NewLine}");
                Console.WriteLine("All tasks have finished.");

                Console.ReadLine();
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                Console.ReadLine();
            }
        }

        private static void ConfigurationSetup()
        {
            var connectionStringSetting = ConfigurationManager.AppSettings["BlobConnectionString"];
            var blobContainerName = ConfigurationManager.AppSettings["BlobContainerName"];

            if (string.IsNullOrWhiteSpace(connectionStringSetting))
            {
                throw new ArgumentException("ConnectionString setting must be set before uploading to storage.");
            }

            if (string.IsNullOrWhiteSpace(blobContainerName))
            {
                throw new ArgumentException("Container name must be set before uploading to storage.");
            }

            var account = CloudStorageAccount.Parse(connectionStringSetting);
            var client = account.CreateCloudBlobClient();

            if (client == null)
            {
                throw new NullReferenceException("CloudBlobClient must be initialized before use");
            }

            _container = client.GetContainerReference(blobContainerName);
            _container.CreateIfNotExists();

            _pathPrefix = ConfigurationManager.AppSettings["BlobUrlPrefix"];

            if (string.IsNullOrWhiteSpace(_pathPrefix))
            {
                throw new ArgumentException("Container prefix must be set before uploading to storage.");
            }

            var startDate = ConfigurationManager.AppSettings["TimePeriodByDate_Start_Utc"];
            var endDate = ConfigurationManager.AppSettings["TimePeriodByDate_End_Utc"];

            var hasStart = DateTimeOffset.TryParse(startDate, out _minTimePeriod);
            var hasEnd = DateTimeOffset.TryParse(endDate, out _maxTimePeriod);

            if (!hasStart)
            {
                throw new ArgumentException("Invalid value of TimePeriodByDate_Start_Utc setting. Please, setup date value.");
            }

            if (!hasEnd)
            {
                throw new ArgumentException("Invalid value of TimePeriodByDate_End_Utc setting. Please, setup date value.");
            }

            if (_minTimePeriod > _maxTimePeriod)
            {
                throw new ArgumentException($"TimePeriodByDate_Start_Utc value is greater than TimePeriodByDate_End_Utc.");
            }

            var targetHours = (int)(_maxTimePeriod - _minTimePeriod).TotalHours + 25;
            _blobPathsSource = new List<string>(targetHours);

            var alertType = ConfigurationManager.AppSettings["AlertsType"];
            _alertType = string.IsNullOrWhiteSpace(alertType) ? ActiveAlertType.All : (ActiveAlertType)Enum.Parse(typeof(ActiveAlertType), alertType);

            _alertName = ConfigurationManager.AppSettings["AlertName"];

            var alertTenantId = ConfigurationManager.AppSettings["AlertTenantId"];
            Guid temp;
            if (Guid.TryParse(alertTenantId, out temp))
            {
                _alertTenantId = temp;
            }


            var blobType = ConfigurationManager.AppSettings["BlobsType"];
            _blobType = string.IsNullOrWhiteSpace(blobType) ? BlobType.None : (BlobType)Enum.Parse(typeof(BlobType), blobType);

            var alertExpirationPolicy = ConfigurationManager.AppSettings["AlertExpirationPolicy"];
            _alertExpirationPolicy = string.IsNullOrWhiteSpace(alertExpirationPolicy) ? AlertExpirationPolicy.None : (AlertExpirationPolicy)Enum.Parse(typeof(AlertExpirationPolicy), alertExpirationPolicy);

            switch (_alertExpirationPolicy)
            {
                case AlertExpirationPolicy.DoNotPushExpired:
                case AlertExpirationPolicy.PushExpiredWithExpirationOverride:
                case AlertExpirationPolicy.PushAll:
                    break;
                case AlertExpirationPolicy.None:
                default: throw new ArgumentException("AlertExpirationPolicy setting must be set");
            }

            var alertExpirationShiftingInDays = ConfigurationManager.AppSettings["AlertExpirationShiftingInDays"];
            var hasAlertExpirationShiftingInDays = long.TryParse(alertExpirationShiftingInDays, out _alertExpirationShiftingInDays);

            if (!hasAlertExpirationShiftingInDays)
            {
                throw new NullReferenceException("AlertExpirationShiftingInDays must be initialized before use (number)");
            }

            _targetSearchServiceName = ConfigurationManager.AppSettings["TargetSearchServiceName"];
            _targetAdminKey = ConfigurationManager.AppSettings["TargetAdminKey"];
            _targetIndexName = ConfigurationManager.AppSettings["TargetIndexName"];

            if (string.IsNullOrWhiteSpace(_targetSearchServiceName) || string.IsNullOrWhiteSpace(_targetAdminKey) || string.IsNullOrWhiteSpace(_targetIndexName))
            {
                throw new ArgumentException("Configuration is wrong. Please, check app settings: TargetSearchServiceName, TargetAdminKey, TargetIndexName values cann't be empty.");
            }

            _targetIndexClient = new SearchIndexClient(new Uri($"https://{_targetSearchServiceName}.search.windows.net"), new AzureKeyCredential(_targetAdminKey));

            if (_targetIndexClient == null)
            {
                throw new NullReferenceException("TargetIndexClient must be initialized before use");
            }

            var timeOutInSeconds = ConfigurationManager.AppSettings["TimeOutInSeconds"];
            _timeOutInSeconds = long.Parse(timeOutInSeconds);

            if (_timeOutInSeconds <= 0)
            {
                throw new ArgumentException($"Invalid value of TimeOutInSeconds setting. Please, setup value more than zero.");
            }

            var alertsBatchSize = ConfigurationManager.AppSettings["AlertsBatchSize"];
            _alertsBatchSize = int.Parse(alertsBatchSize);

            if (_alertsBatchSize <= 0)
            {
                throw new ArgumentException($"Invalid value of TimeOutInSeconds setting. Please, setup value more than zero.");
            }

            Console.WriteLine("*** CONFIGURATION ***");
            Console.WriteLine($"Path of Cloud Blob Storage for alerts: {_pathPrefix}");
            Console.WriteLine($"Period of time: from {_minTimePeriod} to {_maxTimePeriod}");
            Console.WriteLine($"Target service: {_targetSearchServiceName}");
            Console.WriteLine($"Target index: {_targetIndexName}");
        }

        private static void ParseBlobPaths()
        {
            var date = _minTimePeriod;

            while (date <= _maxTimePeriod)
            {
                _blobPathsSource.Add($"{date.Year}/{date.Month:d2}/{date.Day:d2}/{date.Hour:d2}");
                if (date.Day < 10 || date.Hour < 10)
                {
                    _blobPathsSource.Add($"{date.Year}/{date.Month:d2}/{date.Day:d}/{date.Hour:d}/");
                }
                date = date.AddHours(1);
            }
        }

        private static void IncrementStatistic(StatisticType statistic, int increment)
        {
            switch (statistic)
            {
                case StatisticType.BufferedAlertsByNow:
                    Interlocked.Add(ref _bufferedByNow, increment);
                    break;
                case StatisticType.PushedAlertsToAzureByNow:
                    Interlocked.Add(ref _pushedToAzureByNow, increment);
                    break;
                case StatisticType.BlobsDownloadedByNow:
                    Interlocked.Add(ref _blobsDownloadedByNow, increment);
                    break;
            }
        }

        private static void ShowStatistics()
        {

            Console.WriteLine($"    Total Buffered: {_bufferedByNow}\r\n    Total Blobs Downloaded: {_blobsDownloadedByNow}\r\n    Total Alerts Pushed To Azure: {_pushedToAzureByNow}\r\n    Batch block: {_batchBlock.OutputCount}");
        }

        private static async Task RecoverBlobs()
        {
            Observable.Timer(TimeSpan.FromSeconds(30), TimeSpan.FromSeconds(30)).Subscribe(x =>
            {
                if (_bufferedByNow - _pushedToAzureByNow < 300)
                {
                    if (!_cancellationSource.IsCancellationRequested)
                    {
                        _cancellationSource.Cancel();
                        Console.WriteLine($"  {DateTime.Now.ToString("HH:mm")} Downloading Pause cancellation.");
                    }
                }
            });

            _actionBlock = new ActionBlock<ActiveAlertAzureSearchModel[]>(async alerts =>
            {
                //set flag to a false due to Batch was propogated for saving in automatic mode so no manual propogating is required
                try
                {
                    _forcedTriggerBatchRequired = false;
                    var results = await PushAlerts(alerts).ConfigureAwait(false);
                    if (results.Any(result => !result.Succeeded))
                        Console.WriteLine($"    Push to Azure failures {results.Count(result => !result.Succeeded)}");
                    IncrementStatistic(StatisticType.PushedAlertsToAzureByNow, alerts.Length);
                    if (_bufferedByNow - _pushedToAzureByNow < 300)
                    {
                        if (!_cancellationSource.IsCancellationRequested)
                        {
                            _cancellationSource.Cancel();
                            Console.WriteLine($"  {DateTime.Now.ToString("HH:mm")} Downloading Pause has been canceled from actionblock.");
                        }
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.ToString());
                }
            }, new ExecutionDataflowBlockOptions { MaxDegreeOfParallelism = 3, BoundedCapacity = 10000 });

            _batchBlock.LinkTo(_actionBlock);

            foreach (var path in _blobPathsSource)
            {
                _cancellationSource = new CancellationTokenSource();
                if (_bufferedByNow - _pushedToAzureByNow > 10000)
                {
                    Console.WriteLine($"  {DateTime.Now.ToString("HH:mm")} Downloading paused due to actionBlock performance lack");
                    _cancellationSource.Token.WaitHandle.WaitOne();
                    Console.WriteLine($"  {DateTime.Now.ToString("HH:mm")} Downloading continued due");
                }

                Console.WriteLine($"{Environment.NewLine}");

                Console.WriteLine($"  Current path:  {path}");

                var currentPath = $"{_pathPrefix}/{path}";

                var blobRequestOptions = new BlobRequestOptions
                {
                    MaximumExecutionTime = TimeSpan.FromSeconds(_timeOutInSeconds)
                };

                Func<CloudBlockBlob, bool> blobTierFilter;

                switch (_blobType)
                {
                    case BlobType.None:
                    case BlobType.All:
                        blobTierFilter = (_) => _.Properties.StandardBlobTier == StandardBlobTier.Hot || _.Properties.StandardBlobTier == StandardBlobTier.Cool;
                        break;
                    case BlobType.Hot:
                        blobTierFilter = (_) => _.Properties.StandardBlobTier == StandardBlobTier.Cool;
                        break;
                    case BlobType.Cool:
                        blobTierFilter = (_) => _.Properties.StandardBlobTier == StandardBlobTier.Hot;
                        break;
                    default: throw new ArgumentException("BlobsType setting must be set");
                }

                var blobs = _container.ListBlobs(currentPath, useFlatBlobListing: true, options: blobRequestOptions)
                                   .OfType<CloudBlockBlob>()
                                   .Where(_ => blobTierFilter(_))
                                   .ToList();

                if (blobs.Count == 0)
                {
                    Console.WriteLine($"  {DateTime.Now.ToString("HH:mm")} No hot or cool blobs have been found.");
                }
                Console.WriteLine($"  {DateTime.Now.ToString("HH:mm")} {blobs.Count} Hot/Cool blobs were found.");


                var archiveBlobs = _container.ListBlobs(currentPath, useFlatBlobListing: true, options: blobRequestOptions)
                                             .OfType<CloudBlockBlob>()
                                             .Where(_ => _.Properties.StandardBlobTier == StandardBlobTier.Archive)
                                             .ToList();

                if (archiveBlobs.Count > 0)
                {
                    Console.WriteLine($"  We found {archiveBlobs.Count} archived blobs. If you want to recover them, re-hydrate to Hot or Cool state through portal.azure.com");
                    Console.WriteLine("  Continue (Yes/No)?..");
                    var input = Console.ReadLine();

                    if (!input.Equals("Y") && !input.Equals("y") && !input.Equals("Yes") && !input.Equals("yes"))
                    {
                        break;
                    }
                }
                else
                {
                    Console.WriteLine($"  No archived blobs have been found.");
                }

                if (blobs.Count > 0)
                {
                    foreach (var blob in blobs)
                    {
                        Console.WriteLine($"  {DateTime.Now.ToString("hh:mm:ss")} Downloading alerts from blob ...{blob.Name.Substring(40)}");
                        var allAlertsFromBlob = await PullAlerts(blob).ConfigureAwait(false);
                        Console.WriteLine($"  {DateTime.Now.ToString("hh:mm:ss")} Downloaded {allAlertsFromBlob.Count} alerts from blob");
                        IncrementStatistic(StatisticType.BlobsDownloadedByNow, 1);
                        Console.WriteLine($"  {DateTime.Now.ToString("hh:mm:ss")} Filtering and Buffering alerts");
                        var bufferedAlertsCounter = 0;
                        foreach (var blobAlert in allAlertsFromBlob)
                        {
                            if (ShouldSend(blobAlert))
                            {
                                await _batchBlock.SendAsync(blobAlert).ConfigureAwait(false);
                                bufferedAlertsCounter++;
                            }
                        }
                        Console.WriteLine($"  {DateTime.Now.ToString("hh:mm:ss")} Buffered {bufferedAlertsCounter} alerts");
                        Console.WriteLine($"  {DateTime.Now.ToString("hh:mm:ss")} ActionBlock input count: {_actionBlock.InputCount}");

                        IncrementStatistic(StatisticType.BufferedAlertsByNow, bufferedAlertsCounter);

                        ShowStatistics();
                    }
                }
            }

            _batchBlock.TriggerBatch();
            _batchBlock.Complete();
            _batchBlock.Completion.Wait();
            _actionBlock.Complete();
            _actionBlock.Completion.Wait();
            ShowStatistics();
        }

        private static async Task<List<ActiveAlertAzureSearchModel>> PullAlerts(CloudBlockBlob blob)
        {
            var maxRetryAttempts = 2;
            var pauseBetweenFailures = TimeSpan.FromSeconds(10);
            var timeoutInSec = 240;

            //Retry Policy
            var retryPolicy = Policy
                .Handle<Exception>()
                //.Or<AnyOtherException>()
                .WaitAndRetryAsync(
                    maxRetryAttempts,
                    i => pauseBetweenFailures,
                    (exception, timeSpan, retryCount, context) => ManageRetryException(exception, timeSpan, retryCount, context));

            //TimeOut Policy
            var timeOutPolicy = Polly.Policy
                .TimeoutAsync(
                    timeoutInSec,
                    TimeoutStrategy.Pessimistic,
                    (context, timeSpan, task) => ManageTimeoutException(context, timeSpan, task));

            //Combine the two (or more) policies
            var policyWrap = Policy.WrapAsync(retryPolicy, timeOutPolicy);
            string jsonBlob = String.Empty;
            //Execute the transient task(s)
            await policyWrap.ExecuteAsync(async (ct) =>
            {
                jsonBlob = await blob.DownloadTextAsync().ConfigureAwait(false);
            }, new Dictionary<string, object>() { { "ExecuteOperation", "Operation description..." } }).ConfigureAwait(false);


            //var json = blob.DownloadText();
            var alerts = JsonConvert.DeserializeObject<List<ActiveAlertAzureSearchModel>>(jsonBlob);
            return alerts;
        }

        private static void ManageRetryException(Exception exception, TimeSpan timeSpan, int retryCount, Context context)
        {
            var action = context != null ? context.First().Key : "unknown method";
            var actionDescription = context != null ? context.First().Value : "unknown description";
            var msg = $"Retry n°{retryCount} of {action} ({actionDescription}) : {exception.Message}";
            Console.WriteLine(msg);
        }

        private static Task ManageTimeoutException(Context context, TimeSpan timeSpan, Task task)
        {
            var action = context != null ? context.First().Key : "unknown method";
            var actionDescription = context != null ? context.First().Value : "unknown description";

            task.ContinueWith(t =>
            {
                if (t.IsFaulted)
                {
                    var msg = $"Running {action} ({actionDescription}) but the execution timed out after {timeSpan.TotalSeconds} seconds, eventually terminated with: {t.Exception}.";
                    Console.WriteLine(msg);
                }
                else if (t.IsCanceled)
                {
                    var msg = $"Running {action} ({actionDescription}) but the execution timed out after {timeSpan.TotalSeconds} seconds, task cancelled.";
                    Console.WriteLine(msg);
                }
            });

            return task;
        }

        private static bool ShouldSend(ActiveAlertAzureSearchModel alert)
        {
            var shouldSend = true;

            if (_alertType != ActiveAlertType.All)
            {
                shouldSend = alert.activeAlertTypeId == (int)_alertType;
                if (!shouldSend) return false;
            }

            if (!string.IsNullOrEmpty(_alertName))
            {
                shouldSend = alert.eventCode.Equals(_alertName, StringComparison.InvariantCultureIgnoreCase);
                if (!shouldSend) return false;
            }


            if (_alertTenantId.HasValue)
            {
                if (alert.tenantId.HasValue)
                { 
                    shouldSend = alert.tenantId.Value == _alertTenantId.Value;
                    if (!shouldSend) return false;
                }
                else
                {
                    return false;
                };
            }

            switch (_alertExpirationPolicy)
            {
                case AlertExpirationPolicy.DoNotPushExpired:
                    shouldSend = alert.expirationTicks > DateTimeOffset.UtcNow.Ticks;
                    if (!shouldSend) return false;
                    break;
                case AlertExpirationPolicy.PushExpiredWithExpirationOverride:
                    if (alert.expirationTicks < DateTimeOffset.UtcNow.Ticks) 
                    {
                        alert.expirationTicks = DateTimeOffset.UtcNow.Ticks + TimeSpan.TicksPerDay * _alertExpirationShiftingInDays;
                    }
                    break;
                case AlertExpirationPolicy.PushAll:
                case AlertExpirationPolicy.None:
                default: throw new ArgumentException("AlertExpirationPolicy setting must be set");
            }
            return shouldSend;
        }

        private static async Task<IEnumerable<IndexingResult>> PushAlerts(ActiveAlertAzureSearchModel[] alerts)
        {
            var response = await _targetIndexClient.GetSearchClient(_targetIndexName).UploadDocumentsAsync(alerts).ConfigureAwait(false);
            return response.Value.Results;
        }
    }
}
