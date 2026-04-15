import type { RequestHandler } from 'express';
import type { LocalConfig } from '../config/environment.js';

export function createConfigHandler(config: LocalConfig): RequestHandler {
  return (_req, res) => {
    res.status(200).json({
      awsAccountId: "local",
      awsAccountName: "Nile Local",
      awsRegion: "local",
      teamName: config.local.teamName,
      defaultDatabase: config.local.teamName,
      mode: "local",
      localMode: true,
      environment: "local",
      aiMode: config.ai.mode,
      aiProvider: config.ai.provider,
      compute: {
        defaultEngine: config.compute.defaultEngine,
        sparkEnabled: config.compute.sparkEnabled,
      },
      user: {
        name: "local",
      },
    });
  };
}
