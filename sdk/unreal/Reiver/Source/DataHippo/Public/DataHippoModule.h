// Copyright DataHippo. All Rights Reserved.

#pragma once

#include "CoreMinimal.h"
#include "Modules/ModuleManager.h"

/**
 * DataHippo SDK Module
 * Provides game observability with OpenTelemetry v1.39.0 semantic conventions.
 */
class FDataHippoModule : public IModuleInterface
{
public:
    /** IModuleInterface implementation */
    virtual void StartupModule() override;
    virtual void ShutdownModule() override;

    /**
     * Get the DataHippo module instance.
     */
    static FDataHippoModule& Get();

    /**
     * Check if the module is available.
     */
    static bool IsAvailable();
};
