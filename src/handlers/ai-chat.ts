import type { RequestHandler } from 'express';
import type { SQLiteService } from '../services/sqlite-service.js';
import type {
  ChatSession,
  CompactedRange,
  ListSessionsResponse,
  GetSessionResponse,
  DeleteSessionResponse,
  StarSessionResponse,
  GetCompactedRangesResponse,
  SessionSummary,
} from '../types/types.js';

/**
 * Creates all AI chat session CRUD handlers.
 * Both /ai-chat/sessions and /ai/sessions routes share the same logic.
 */
export function createAiChatHandlers(sqliteService: SQLiteService): {
  listSessions: RequestHandler;
  getSession: RequestHandler;
  deleteSession: RequestHandler;
  starSession: RequestHandler;
  getCompactedRanges: RequestHandler;
  saveCompactedRange: RequestHandler;
  updateSession: RequestHandler;
  generateTitle: RequestHandler;
} {
  // ── List Sessions ────────────────────────────────────────────
  const listSessions: RequestHandler = (_req, res) => {
    try {
      const starred = _req.query['starred'] !== undefined
        ? _req.query['starred'] === 'true'
        : undefined;
      const rows = sqliteService.listChatSessions(starred);
      const sessions: SessionSummary[] = rows.map((r) => ({
        sessionId: r.sessionId,
        title: r.title,
        starred: r.starred,
        updatedAt: r.updatedAt,
        messageCount: r.messageCount,
        lastMessagePreview: r.lastMessagePreview,
      }));
      const response: ListSessionsResponse = { sessions };
      res.json(response);
    } catch (err) {
      console.error('[ai-chat] Error listing sessions:', err);
      res.status(500).json({ error: 'Failed to list chat sessions' });
    }
  };

  // ── Get Session ──────────────────────────────────────────────
  const getSession: RequestHandler = (req, res) => {
    try {
      const sessionId = req.params['sessionId'];
      if (!sessionId) {
        res.status(400).json({ error: 'Missing sessionId parameter' });
        return;
      }
      const row = sqliteService.getChatSession(sessionId);
      if (!row) {
        res.status(404).json({ error: 'Session not found', sessionId });
        return;
      }

      let messages: ChatSession['messages'] = [];
      try { messages = JSON.parse(row.messagesJson); } catch { /* empty */ }

      let context: ChatSession['context'] = {};
      try { context = JSON.parse(row.contextJson); } catch { /* empty */ }

      const session: ChatSession = {
        sessionId: row.sessionId,
        userId: row.userId,
        messages,
        context,
        starred: row.starred,
        title: row.title,
        createdAt: row.createdAt,
        updatedAt: row.updatedAt,
        compactionCount: row.compactionCount,
      };
      const response: GetSessionResponse = { session };
      res.json(response);
    } catch (err) {
      console.error('[ai-chat] Error getting session:', err);
      res.status(500).json({ error: 'Failed to get chat session' });
    }
  };

  // ── Delete Session ───────────────────────────────────────────
  const deleteSession: RequestHandler = (req, res) => {
    try {
      const sessionId = req.params['sessionId'];
      if (!sessionId) {
        res.status(400).json({ error: 'Missing sessionId parameter' });
        return;
      }
      sqliteService.deleteChatSession(sessionId);
      const response: DeleteSessionResponse = {
        success: true,
        message: `Session ${sessionId} deleted`,
      };
      res.json(response);
    } catch (err) {
      console.error('[ai-chat] Error deleting session:', err);
      res.status(500).json({ error: 'Failed to delete chat session' });
    }
  };

  // ── Star Session ─────────────────────────────────────────────
  const starSession: RequestHandler = (req, res) => {
    try {
      const sessionId = req.params['sessionId'];
      if (!sessionId) {
        res.status(400).json({ error: 'Missing sessionId parameter' });
        return;
      }
      const body = req.body as { starred?: boolean } | undefined;
      const starred = body?.starred ?? false;
      const updated = sqliteService.starChatSession(sessionId, starred);
      if (!updated) {
        res.status(404).json({ error: 'Session not found', sessionId });
        return;
      }
      const response: StarSessionResponse = {
        success: true,
        message: starred ? 'Session starred' : 'Session unstarred',
      };
      res.json(response);
    } catch (err) {
      console.error('[ai-chat] Error starring session:', err);
      res.status(500).json({ error: 'Failed to star chat session' });
    }
  };

  // ── Get Compacted Ranges ─────────────────────────────────────
  const getCompactedRanges: RequestHandler = (req, res) => {
    try {
      const sessionId = req.params['sessionId'];
      if (!sessionId) {
        res.status(400).json({ error: 'Missing sessionId parameter' });
        return;
      }
      const rangesJson = sqliteService.getChatCompactedRanges(sessionId);
      let ranges: CompactedRange[] = [];
      try { ranges = JSON.parse(rangesJson); } catch { /* empty */ }
      const response: GetCompactedRangesResponse = { ranges };
      res.json(response);
    } catch (err) {
      console.error('[ai-chat] Error getting compacted ranges:', err);
      res.status(500).json({ error: 'Failed to get compacted ranges' });
    }
  };

  // ── Save Compacted Range ─────────────────────────────────────
  const saveCompactedRange: RequestHandler = (req, res) => {
    try {
      const sessionId = req.params['sessionId'];
      if (!sessionId) {
        res.status(400).json({ error: 'Missing sessionId parameter' });
        return;
      }
      const body = req.body as Partial<CompactedRange> | undefined;
      if (!body) {
        res.status(400).json({ error: 'Missing request body' });
        return;
      }
      // Build the full range with sessionId and a state key
      const range: CompactedRange = {
        sessionId,
        state: body.state || `COMPACTED#${new Date().toISOString()}`,
        messages: body.messages || [],
        summary: body.summary || '',
        compactedAt: body.compactedAt || new Date().toISOString(),
        messageCount: body.messageCount || (body.messages?.length ?? 0),
      };
      sqliteService.saveChatCompactedRange(sessionId, JSON.stringify(range));
      res.json({ success: true });
    } catch (err) {
      console.error('[ai-chat] Error saving compacted range:', err);
      res.status(500).json({ error: 'Failed to save compacted range' });
    }
  };

  // ── Update Session (PUT /ai/sessions/{sessionId}) ────────────
  // This is the main save endpoint used by the client (session-manager.ts)
  const updateSession: RequestHandler = (req, res) => {
    try {
      const sessionId = req.params['sessionId'];
      if (!sessionId) {
        res.status(400).json({ error: 'Missing sessionId parameter' });
        return;
      }
      const body = req.body as Record<string, unknown> | undefined;
      if (!body) {
        res.status(400).json({ error: 'Missing request body' });
        return;
      }

      // The client sends the full ChatSession object
      const messages = body['messages'] as unknown[] | undefined;
      const messagesJson = JSON.stringify(messages ?? []);
      const title = (body['title'] as string | undefined) ?? undefined;
      const userId = (body['userId'] as string | undefined) ?? 'local';
      const starred = (body['starred'] as boolean | undefined) ?? false;
      const context = body['context'] as Record<string, unknown> | undefined;
      const contextJson = JSON.stringify(context ?? {});
      const compactionCount = (body['compactionCount'] as number | undefined) ?? 0;
      const createdAt = (body['createdAt'] as string | undefined) ?? undefined;
      const updatedAt = (body['updatedAt'] as string | undefined) ?? new Date().toISOString();

      sqliteService.upsertChatSession(sessionId, title, messagesJson, {
        userId,
        starred,
        contextJson,
        compactionCount,
        createdAt,
        updatedAt,
      });

      res.json({ success: true });
    } catch (err) {
      console.error('[ai-chat] Error updating session:', err);
      res.status(500).json({ error: 'Failed to save chat session' });
    }
  };

  // ── Generate Title (POST /ai/sessions/{sessionId}/title) ─────
  // In local mode, we generate a simple title from messages (no AI)
  const generateTitle: RequestHandler = (req, res) => {
    try {
      const sessionId = req.params['sessionId'];
      if (!sessionId) {
        res.status(400).json({ error: 'Missing sessionId parameter' });
        return;
      }
      const row = sqliteService.getChatSession(sessionId);
      if (!row) {
        res.status(404).json({ error: 'Session not found', sessionId });
        return;
      }

      let messages: Array<{ role?: string; content?: string | unknown[] }> = [];
      try { messages = JSON.parse(row.messagesJson); } catch { /* empty */ }

      // Find first user message and use first 6 words as title
      const firstUserMsg = messages.find(m => m.role === 'user');
      let title = 'AI Chat';
      if (firstUserMsg && typeof firstUserMsg.content === 'string') {
        const words = firstUserMsg.content.split(/\s+/).slice(0, 6);
        title = words.join(' ') + (firstUserMsg.content.split(/\s+/).length > 6 ? '...' : '');
      }

      // Save the generated title
      sqliteService.upsertChatSession(sessionId, title, row.messagesJson, {
        userId: row.userId,
        starred: row.starred,
        contextJson: row.contextJson,
        compactionCount: row.compactionCount,
        createdAt: row.createdAt,
        updatedAt: new Date().toISOString(),
      });

      res.json({ title });
    } catch (err) {
      console.error('[ai-chat] Error generating title:', err);
      res.status(500).json({ error: 'Failed to generate title' });
    }
  };

  return {
    listSessions,
    getSession,
    deleteSession,
    starSession,
    getCompactedRanges,
    saveCompactedRange,
    updateSession,
    generateTitle,
  };
}
