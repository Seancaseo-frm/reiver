// Copyright DataHippo. All Rights Reserved.

#include "DataHippoSubsystem.h"
#include "Engine/Engine.h"
#include "HAL/PlatformMisc.h"
#include "HAL/PlatformProcess.h"
#include "Misc/App.h"
#include "Misc/Guid.h"
#include "GenericPlatform/GenericPlatformMemory.h"
#include "Serialization/JsonSerializer.h"
#include "HttpModule.h"
#include "Interfaces/IHttpRequest.h"
#include "Interfaces/IHttpResponse.h"

void UDataHippoSubsystem::Initialize(FSubsystemCollectionBase& Collection)
{
    Super::Initialize(Collection);
    
    // Pre-allocate ring buffers to avoid runtime allocations
    TickTimeBuffer.SetNumZeroed(BufferSize);
    RttBuffer.SetNumZeroed(BufferSize);
    PacketLossBuffer.SetNumZeroed(BufferSize);
    
    // Reset ring buffer indices
    TickTimeIndex = 0;
    TickTimeCount = 0;
    RttIndex = 0;
    RttCount = 0;
    PacketLossIndex = 0;
    PacketLossCount = 0;
}

void UDataHippoSubsystem::Deinitialize()
{
    if (bIsInitialized)
    {
        ShutdownSDK();
    }
    Super::Deinitialize();
}

void UDataHippoSubsystem::InitializeSDK(const FDataHippoConfig& InConfig)
{
    if (bIsInitialized)
    {
        UE_LOG(LogTemp, Warning, TEXT("[DataHippo] SDK already initialized"));
        return;
    }

    if (InConfig.ProjectKey.IsEmpty())
    {
        UE_LOG(LogTemp, Error, TEXT("[DataHippo] Project key is required"));
        return;
    }

    Config = InConfig;

    // Generate session ID
    SessionId = FGuid::NewGuid().ToString();
    SessionStartTime = FDateTime::UtcNow();

    // Start tick callback for metrics collection
    TickHandle = FTSTicker::GetCoreTicker().AddTicker(
        FTickerDelegate::CreateUObject(this, &UDataHippoSubsystem::Tick),
        Config.MetricsIntervalSeconds
    );

    StartSession();

    bIsInitialized = true;
    UE_LOG(LogTemp, Log, TEXT("[DataHippo] SDK initialized, session: %s"), *SessionId);
}

void UDataHippoSubsystem::ShutdownSDK()
{
    if (!bIsInitialized) return;

    // Remove tick callback
    FTSTicker::GetCoreTicker().RemoveTicker(TickHandle);

    EndSession(TEXT("normal"));
    SendTelemetry();

    bIsInitialized = false;
    UE_LOG(LogTemp, Log, TEXT("[DataHippo] SDK shutdown complete"));
}

bool UDataHippoSubsystem::Tick(float DeltaTime)
{
    if (!bIsInitialized) return true;

    // Collect tick metrics
    if (Config.bEnableTickMetrics)
    {
        CollectTickMetrics();
    }

    TimeSinceLastReport += DeltaTime;

    if (TimeSinceLastReport >= Config.MetricsIntervalSeconds)
    {
        if (Config.bEnableMemoryMetrics)
        {
            CollectMemoryMetrics();
        }

        // Report aggregated metrics
        ReportAggregatedMetrics();

        TimeSinceLastReport = 0.0f;
    }

    // Periodically send queued telemetry
    if (TelemetryQueue.Num() >= 100)
    {
        SendTelemetry();
    }

    return true;
}

void UDataHippoSubsystem::StartSession()
{
    TSharedPtr<FJsonObject> Event = MakeShareable(new FJsonObject());
    Event->SetStringField(TEXT("name"), TEXT("game.session.start"));
    Event->SetStringField(TEXT("game.session.id"), SessionId);
    Event->SetStringField(TEXT("game.version"), FApp::GetProjectVersion());
    Event->SetStringField(TEXT("game.platform"), GetPlatformString());
    Event->SetStringField(TEXT("device.model.name"), FPlatformMisc::GetDeviceMakeAndModel());
    Event->SetStringField(TEXT("os.description"), FPlatformMisc::GetOSVersion());

    {
        FScopeLock Lock(&QueueLock);
        TelemetryQueue.Add(Event);
    }
}

void UDataHippoSubsystem::EndSession(const FString& Reason)
{
    FTimespan Duration = FDateTime::UtcNow() - SessionStartTime;

    TSharedPtr<FJsonObject> Event = MakeShareable(new FJsonObject());
    Event->SetStringField(TEXT("name"), TEXT("game.session.end"));
    Event->SetStringField(TEXT("game.session.id"), SessionId);
    Event->SetNumberField(TEXT("game.session.duration"), Duration.GetTotalSeconds());
    Event->SetStringField(TEXT("game.session.end_reason"), Reason);

    if (!PlayerId.IsEmpty())
    {
        Event->SetStringField(TEXT("game.player.id"), PlayerId);
    }

    {
        FScopeLock Lock(&QueueLock);
        TelemetryQueue.Add(Event);
    }
}

void UDataHippoSubsystem::StartMatch(const FString& MatchId, const FString& Mode, const FString& Map)
{
    if (!bIsInitialized) return;

    CurrentMatchId = MatchId;

    TSharedPtr<FJsonObject> Event = MakeShareable(new FJsonObject());
    Event->SetStringField(TEXT("name"), TEXT("game.match.start"));
    Event->SetStringField(TEXT("game.match.id"), MatchId);
    Event->SetStringField(TEXT("game.session.id"), SessionId);

    if (!Mode.IsEmpty())
    {
        Event->SetStringField(TEXT("game.match.mode"), Mode);
    }
    if (!Map.IsEmpty())
    {
        Event->SetStringField(TEXT("game.match.map"), Map);
    }

    {
        FScopeLock Lock(&QueueLock);
        TelemetryQueue.Add(Event);
    }
}

void UDataHippoSubsystem::EndMatch(const FString& Outcome, const FString& WinningTeam)
{
    if (!bIsInitialized || CurrentMatchId.IsEmpty()) return;

    TSharedPtr<FJsonObject> Event = MakeShareable(new FJsonObject());
    Event->SetStringField(TEXT("name"), TEXT("game.match.end"));
    Event->SetStringField(TEXT("game.match.id"), CurrentMatchId);
    Event->SetStringField(TEXT("game.session.id"), SessionId);

    if (!Outcome.IsEmpty())
    {
        Event->SetStringField(TEXT("game.match.outcome"), Outcome);
    }
    if (!WinningTeam.IsEmpty())
    {
        Event->SetStringField(TEXT("game.match.winning_team"), WinningTeam);
    }

    {
        FScopeLock Lock(&QueueLock);
        TelemetryQueue.Add(Event);
    }

    CurrentMatchId.Empty();
}

void UDataHippoSubsystem::SetPlayerId(const FString& InPlayerId)
{
    PlayerId = InPlayerId;
}

void UDataHippoSubsystem::RecordPlayerJoin(const FString& Team)
{
    if (!bIsInitialized || CurrentMatchId.IsEmpty()) return;

    TSharedPtr<FJsonObject> Event = MakeShareable(new FJsonObject());
    Event->SetStringField(TEXT("name"), TEXT("game.player.join"));
    Event->SetStringField(TEXT("game.match.id"), CurrentMatchId);
    Event->SetStringField(TEXT("game.session.id"), SessionId);

    if (!Team.IsEmpty())
    {
        Event->SetStringField(TEXT("game.player.team"), Team);
    }

    {
        FScopeLock Lock(&QueueLock);
        TelemetryQueue.Add(Event);
    }
}

void UDataHippoSubsystem::RecordPlayerLeave(const FString& Reason)
{
    if (!bIsInitialized || CurrentMatchId.IsEmpty()) return;

    TSharedPtr<FJsonObject> Event = MakeShareable(new FJsonObject());
    Event->SetStringField(TEXT("name"), TEXT("game.player.leave"));
    Event->SetStringField(TEXT("game.match.id"), CurrentMatchId);
    Event->SetStringField(TEXT("game.session.id"), SessionId);

    if (!Reason.IsEmpty())
    {
        Event->SetStringField(TEXT("game.player.leave_reason"), Reason);
    }

    {
        FScopeLock Lock(&QueueLock);
        TelemetryQueue.Add(Event);
    }
}

void UDataHippoSubsystem::ReportNetworkRtt(float RttSeconds)
{
    if (!bIsInitialized) return;
    AddToRttBuffer(RttSeconds);
}

void UDataHippoSubsystem::AddToRttBuffer(float Value)
{
    // O(1) ring buffer insertion
    RttBuffer[RttIndex] = Value;
    RttIndex = (RttIndex + 1) % BufferSize;
    if (RttCount < BufferSize)
    {
        RttCount++;
    }
}

void UDataHippoSubsystem::ReportPacketLoss(float LossRatio)
{
    if (!bIsInitialized) return;
    LossRatio = FMath::Clamp(LossRatio, 0.0f, 1.0f);
    AddToPacketLossBuffer(LossRatio);
}

void UDataHippoSubsystem::AddToPacketLossBuffer(float Value)
{
    // O(1) ring buffer insertion
    PacketLossBuffer[PacketLossIndex] = Value;
    PacketLossIndex = (PacketLossIndex + 1) % BufferSize;
    if (PacketLossCount < BufferSize)
    {
        PacketLossCount++;
    }
}

void UDataHippoSubsystem::RecordGauge(const FString& Name, float Value)
{
    if (!bIsInitialized) return;

    TSharedPtr<FJsonObject> Metric = MakeShareable(new FJsonObject());
    Metric->SetStringField(TEXT("type"), TEXT("gauge"));
    Metric->SetStringField(TEXT("name"), Name);
    Metric->SetNumberField(TEXT("value"), Value);

    if (!CurrentMatchId.IsEmpty())
    {
        Metric->SetStringField(TEXT("game.match.id"), CurrentMatchId);
    }

    {
        FScopeLock Lock(&QueueLock);
        TelemetryQueue.Add(Metric);
    }
}

void UDataHippoSubsystem::RecordHistogram(const FString& Name, float Value)
{
    if (!bIsInitialized) return;

    TSharedPtr<FJsonObject> Metric = MakeShareable(new FJsonObject());
    Metric->SetStringField(TEXT("type"), TEXT("histogram"));
    Metric->SetStringField(TEXT("name"), Name);
    Metric->SetNumberField(TEXT("value"), Value);

    if (!CurrentMatchId.IsEmpty())
    {
        Metric->SetStringField(TEXT("game.match.id"), CurrentMatchId);
    }

    {
        FScopeLock Lock(&QueueLock);
        TelemetryQueue.Add(Metric);
    }
}

void UDataHippoSubsystem::IncrementCounter(const FString& Name, int32 Delta)
{
    if (!bIsInitialized) return;

    TSharedPtr<FJsonObject> Metric = MakeShareable(new FJsonObject());
    Metric->SetStringField(TEXT("type"), TEXT("counter"));
    Metric->SetStringField(TEXT("name"), Name);
    Metric->SetNumberField(TEXT("value"), Delta);

    if (!CurrentMatchId.IsEmpty())
    {
        Metric->SetStringField(TEXT("game.match.id"), CurrentMatchId);
    }

    {
        FScopeLock Lock(&QueueLock);
        TelemetryQueue.Add(Metric);
    }
}

void UDataHippoSubsystem::CollectTickMetrics()
{
    // Get current frame time (delta time in seconds per OTel v1.39.0)
    float TickTime = FApp::GetDeltaTime();
    AddToTickBuffer(TickTime);
}

void UDataHippoSubsystem::AddToTickBuffer(float Value)
{
    // O(1) ring buffer insertion
    TickTimeBuffer[TickTimeIndex] = Value;
    TickTimeIndex = (TickTimeIndex + 1) % BufferSize;
    if (TickTimeCount < BufferSize)
    {
        TickTimeCount++;
    }
}

void UDataHippoSubsystem::CollectMemoryMetrics()
{
    FPlatformMemoryStats MemStats = FPlatformMemory::GetStats();

    RecordGauge(TEXT("game.client.memory.usage"), static_cast<float>(MemStats.UsedPhysical));
    RecordGauge(TEXT("game.client.memory.available"), static_cast<float>(MemStats.AvailablePhysical));
}

void UDataHippoSubsystem::ReportAggregatedMetrics()
{
    // Report tick/frame metrics using ring buffer count
    if (TickTimeCount > 0)
    {
        float Sum = 0;
        float Min = TNumericLimits<float>::Max();
        float Max = TNumericLimits<float>::Lowest();

        for (int32 i = 0; i < TickTimeCount; i++)
        {
            float TickTime = TickTimeBuffer[i];
            Sum += TickTime;
            Min = FMath::Min(Min, TickTime);
            Max = FMath::Max(Max, TickTime);
        }

        float Avg = Sum / TickTimeCount;
        float AvgFps = 1.0f / FMath::Max(Avg, 0.0001f); // Prevent division by zero

        RecordGauge(TEXT("game.client.frame.rate"), AvgFps);
        RecordHistogram(TEXT("game.client.frame.duration"), Avg);
    }

    // Report network metrics using ring buffer count
    if (RttCount > 0)
    {
        float Sum = 0;
        for (int32 i = 0; i < RttCount; i++)
        {
            Sum += RttBuffer[i];
        }
        float Avg = Sum / RttCount;

        RecordHistogram(TEXT("game.network.rtt"), Avg);

        // Compute jitter (RTT variance)
        float SumSquaredDiff = 0;
        for (int32 i = 0; i < RttCount; i++)
        {
            float Diff = RttBuffer[i] - Avg;
            SumSquaredDiff += Diff * Diff;
        }
        float Jitter = FMath::Sqrt(SumSquaredDiff / RttCount);

        RecordGauge(TEXT("game.network.jitter"), Jitter);
    }

    // Report packet loss using ring buffer count
    if (PacketLossCount > 0)
    {
        float Sum = 0;
        for (int32 i = 0; i < PacketLossCount; i++)
        {
            Sum += PacketLossBuffer[i];
        }
        float Avg = Sum / PacketLossCount;

        RecordGauge(TEXT("game.network.packet_loss"), Avg);
    }
}

void UDataHippoSubsystem::SendTelemetry()
{
    TArray<TSharedPtr<FJsonObject>> Batch;

    {
        FScopeLock Lock(&QueueLock);
        if (TelemetryQueue.Num() == 0) return;

        // Take items from the queue efficiently
        int32 Count = FMath::Min(TelemetryQueue.Num(), 100);
        
        if (Count == TelemetryQueue.Num())
        {
            // Taking all items - just swap the arrays (O(1))
            Batch = MoveTemp(TelemetryQueue);
            TelemetryQueue.Reset();
        }
        else
        {
            // Taking partial items - copy last N items, then truncate
            // This is O(Count) instead of O(TelemetryQueue.Num())
            Batch.Reserve(Count);
            int32 StartIndex = TelemetryQueue.Num() - Count;
            for (int32 i = StartIndex; i < TelemetryQueue.Num(); i++)
            {
                Batch.Add(MoveTemp(TelemetryQueue[i]));
            }
            TelemetryQueue.SetNum(StartIndex, false);
        }
    }

    // Build JSON payload
    TSharedPtr<FJsonObject> Payload = MakeShareable(new FJsonObject());

    TArray<TSharedPtr<FJsonValue>> Items;
    for (const auto& Item : Batch)
    {
        Items.Add(MakeShareable(new FJsonValueObject(Item)));
    }
    Payload->SetArrayField(TEXT("items"), Items);
    Payload->SetStringField(TEXT("session_id"), SessionId);

    FString JsonString;
    TSharedRef<TJsonWriter<>> Writer = TJsonWriterFactory<>::Create(&JsonString);
    FJsonSerializer::Serialize(Payload.ToSharedRef(), Writer);

    // Send HTTP request
    TSharedRef<IHttpRequest, ESPMode::ThreadSafe> Request = FHttpModule::Get().CreateRequest();
    Request->SetURL(Config.ApiUrl + TEXT("/api/v1/ingest"));
    Request->SetVerb(TEXT("POST"));
    Request->SetHeader(TEXT("Content-Type"), TEXT("application/json"));
    Request->SetHeader(TEXT("x-reiver-project-key"), Config.ProjectKey);
    Request->SetContentAsString(JsonString);

    Request->OnProcessRequestComplete().BindLambda([](FHttpRequestPtr Req, FHttpResponsePtr Resp, bool bSuccess)
    {
        if (!bSuccess || !Resp.IsValid())
        {
            UE_LOG(LogTemp, Warning, TEXT("[DataHippo] Failed to send telemetry"));
        }
    });

    Request->ProcessRequest();
}

FString UDataHippoSubsystem::GetPlatformString() const
{
#if PLATFORM_WINDOWS
    return TEXT("windows");
#elif PLATFORM_MAC
    return TEXT("macos");
#elif PLATFORM_LINUX
    return TEXT("linux");
#elif PLATFORM_ANDROID
    return TEXT("android");
#elif PLATFORM_IOS
    return TEXT("ios");
#elif PLATFORM_SWITCH
    return TEXT("switch");
#elif PLATFORM_XBOXONE || PLATFORM_XSX
    return TEXT("xbox");
#elif PLATFORM_PS4 || PLATFORM_PS5
    return TEXT("playstation");
#else
    return TEXT("unknown");
#endif
}
