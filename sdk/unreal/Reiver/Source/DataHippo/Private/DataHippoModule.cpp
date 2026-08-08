// Copyright DataHippo. All Rights Reserved.

#include "DataHippoModule.h"
#include "DataHippoSubsystem.h"

#define LOCTEXT_NAMESPACE "FDataHippoModule"

void FDataHippoModule::StartupModule()
{
    UE_LOG(LogTemp, Log, TEXT("[DataHippo] Module started"));
}

void FDataHippoModule::ShutdownModule()
{
    UE_LOG(LogTemp, Log, TEXT("[DataHippo] Module shutdown"));
}

FDataHippoModule& FDataHippoModule::Get()
{
    return FModuleManager::LoadModuleChecked<FDataHippoModule>("DataHippo");
}

bool FDataHippoModule::IsAvailable()
{
    return FModuleManager::Get().IsModuleLoaded("DataHippo");
}

#undef LOCTEXT_NAMESPACE

IMPLEMENT_MODULE(FDataHippoModule, DataHippo)
