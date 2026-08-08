// Copyright DataHippo. All Rights Reserved.

#pragma once

#include "CoreMinimal.h"
#include "Subsystems/GameInstanceSubsystem.h"
#include "DataHippoSubsystem.generated.h"

/**
 * DataHippo configuration settings.
 */
USTRUCT(BlueprintType)
struct DATAHIPPO_API FDataHippoConfig
{
    GENERATED_BODY()

    /** DataHippo API endpoint URL */
    UPROPERTY(EditAnywhere, BlueprintReadWrite, Category = "DataHippo")
    FString ApiUrl = TEXT("https://api.datahippo.io");

    /** Project API key from DataHippo dashboard */
    UPROPERTY(EditAnywhere, BlueprintReadWrite, Category = "DataHippo")
    FString ProjectKey;

    /** Name of your game */
    UPROPERTY(EditAnywhere, BlueprintReadWrite, Category = "DataHippo")
    FString GameName;

    /** Enable automatic tick rate tracking */
    UPROPERTY(EditAnywhere, BlueprintReadWrite, Category = "DataHippo")
    bool bEnableTickMetrics = true;

    /** Enable automatic memory usage tracking */
    UPROPERTY(EditAnywhere, BlueprintReadWrite, Category = "DataHippo")
    bool bEnableMemoryMetrics = true;

    /** Enable automatic network quality tracking */
    UPROPERTY(EditAnywhere, BlueprintReadWrite, Category = "DataHippo")
    bool bEnableNetworkMetrics = true;

    /** Enable crash reporting */
    UPROPERTY(EditAnywhere, BlueprintReadWrite, Category = "DataHippo")
    bool bEnableCrashReporting = true;

    /** Metrics collection interval in seconds */
    UPROPERTY(EditAnywhere, BlueprintReadWrite, Category = "DataHippo", meta = (ClampMin = "0.1", ClampMax = "10.0"))
    float MetricsIntervalSeconds = 1.0f;

    /** Include device ID in telemetry */
    UPROPERTY(EditAnywhere, BlueprintReadWrite, Category = "DataHippo")
    bool bSendDeviceId = true;
};

/**
 * DataHippo Game Instance Subsystem
 * Manages telemetry collection and export throughout the game session.
 */
UCLASS()
class DATAHIPPO_API UDataHippoSubsystem : public UGameInstanceSubsystem
{
    GENERATED_BODY()

public:
    // USubsystem interface
    virtual void Initialize(FSubsystemCollectionBase& Collection) override;
    virtual void Deinitialize() override;

    /**
     * Initialize the DataHippo SDK with configuration.
     * Call this early in your game's lifecycle.
     */
    UFUNCTION(BlueprintCallable, Category = "DataHippo")
    void InitializeSDK(const FDataHippoConfig& Config);

    /**
     * Shutdown the SDK and flush remaining telemetry.
     */
    UFUNCTION(BlueprintCallable, Category = "DataHippo")
    void ShutdownSDK();

    /**
     * Check if the SDK is initialized.
     */
    UFUNCTION(BlueprintPure, Category = "DataHippo")
    bool IsInitialized() const { return bIsInitialized; }

    /**
     * Get the current session ID.
     */
    UFUNCTION(BlueprintPure, Category = "DataHippo")
    FString GetSessionId() const { return SessionId; }

    /**
     * Get the current match ID (empty if not in a match).
     */
    UFUNCTION(BlueprintPure, Category = "DataHippo")
    FString GetCurrentMatchId() const { return CurrentMatchId; }

    // Match Tracking

    /**
     * Start tracking a match.
     * @param MatchId Unique match identifier
     * @param Mode Game mode (e.g., "ranked", "casual")
     * @param Map Map or level name
     */
    UFUNCTION(BlueprintCallable, Category = "DataHippo|Match")
    void StartMatch(const FString& MatchId, const FString& Mode = TEXT(""), const FString& Map = TEXT(""));

    /**
     * End the current match.
     * @param Outcome Match outcome (e.g., "completed", "abandoned")
     * @param WinningTeam Winning team identifier
     */
    UFUNCTION(BlueprintCallable, Category = "DataHippo|Match")
    void EndMatch(const FString& Outcome = TEXT(""), const FString& WinningTeam = TEXT(""));

    // Player Tracking

    /**
     * Set the current player ID for telemetry correlation.
     * Use a pseudonymous identifier, not PII.
     */
    UFUNCTION(BlueprintCallable, Category = "DataHippo|Player")
    void SetPlayerId(const FString& PlayerId);

    /**
     * Record player joining the current match.
     */
    UFUNCTION(BlueprintCallable, Category = "DataHippo|Player")
    void RecordPlayerJoin(const FString& Team = TEXT(""));

    /**
     * Record player leaving the current match.
     */
    UFUNCTION(BlueprintCallable, Category = "DataHippo|Player")
    void RecordPlayerLeave(const FString& Reason = TEXT(""));

    // Network Quality

    /**
     * Report current network RTT (round-trip time) in seconds.
     * Call this periodically with your game's measured RTT.
     */
    UFUNCTION(BlueprintCallable, Category = "DataHippo|Network")
    void ReportNetworkRtt(float RttSeconds);

    /**
     * Report packet loss ratio (0.0 to 1.0).
     */
    UFUNCTION(BlueprintCallable, Category = "DataHippo|Network")
    void ReportPacketLoss(float LossRatio);

    // Custom Metrics

    /**
     * Record a custom gauge metric.
     */
    UFUNCTION(BlueprintCallable, Category = "DataHippo|Metrics")
    void RecordGauge(const FString& Name, float Value);

    /**
     * Record a custom histogram metric.
     */
    UFUNCTION(BlueprintCallable, Category = "DataHippo|Metrics")
    void RecordHistogram(const FString& Name, float Value);

    /**
     * Increment a counter metric.
     */
    UFUNCTION(BlueprintCallable, Category = "DataHippo|Metrics")
    void IncrementCounter(const FString& Name, int32 Delta = 1);

private:
    void StartSession();
    void EndSession(const FString& Reason);
    void CollectTickMetrics();
    void CollectMemoryMetrics();
    void ReportAggregatedMetrics();
    void SendTelemetry();
    FString GetPlatformString() const;

    // Ring buffer helper functions
    void AddToTickBuffer(float Value);
    void AddToRttBuffer(float Value);
    void AddToPacketLossBuffer(float Value);

    // Tick callback for metrics collection
    bool Tick(float DeltaTime);
    FTSTicker::FDelegateHandle TickHandle;

    FDataHippoConfig Config;
    bool bIsInitialized = false;

    FString SessionId;
    FString CurrentMatchId;
    FString PlayerId;
    FDateTime SessionStartTime;

    // Ring buffer implementation for O(1) insertions
    // Using fixed-size arrays with wrap-around indices
    static constexpr int32 BufferSize = 120;
    static constexpr int32 MaxQueueSize = 1000;

    // Tick time ring buffer
    TArray<float> TickTimeBuffer;
    int32 TickTimeIndex = 0;
    int32 TickTimeCount = 0;

    // RTT ring buffer
    TArray<float> RttBuffer;
    int32 RttIndex = 0;
    int32 RttCount = 0;

    // Packet loss ring buffer
    TArray<float> PacketLossBuffer;
    int32 PacketLossIndex = 0;
    int32 PacketLossCount = 0;

    float TimeSinceLastReport = 0.0f;

    // Telemetry queue
    TArray<TSharedPtr<FJsonObject>> TelemetryQueue;
    FCriticalSection QueueLock;
};
