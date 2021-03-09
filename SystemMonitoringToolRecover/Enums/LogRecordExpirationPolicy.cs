namespace BlobStorageToAzureSearchPush.Enums
{
    internal enum LogRecordExpirationPolicy
    {
        None = 0,
        DoNotPushExpired = 1,
        PushExpiredWithExpirationOverride = 2,
        PushAll = 3
    }
}
