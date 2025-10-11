/** @odoo-module **/
import { registry } from "@web/core/registry";

/**
 * IComply Terminal Service - Multi-profile log viewer
 */
export const icomplyTerminalService = {
    dependencies: ['bus_service', 'rpc'],

    start(env, { bus_service, rpc }) {
        const profileSessions = new Map(); // profileId -> session data
        const MAX_LOGS = 10000;

        function formatTimestamp(date) {
            if (typeof date === 'string') {
                return date;
            }
            const d = new Date(date);
            const year = d.getFullYear();
            const month = String(d.getMonth() + 1).padStart(2, '0');
            const day = String(d.getDate()).padStart(2, '0');
            const hours = String(d.getHours()).padStart(2, '0');
            const minutes = String(d.getMinutes()).padStart(2, '0');
            const seconds = String(d.getSeconds()).padStart(2, '0');
            return `${year}-${month}-${day} ${hours}:${minutes}:${seconds}`;
        }

        function getOrCreateSession(profileId) {
            if (!profileSessions.has(profileId)) {
                profileSessions.set(profileId, {
                    logs: [],
                    listeners: new Set(),
                    filePosition: 0,
                    isPaused: false,
                    pollInterval: null,
                    isInitialized: false,
                    profileInfo: null,
                });
            }
            return profileSessions.get(profileId);
        }

        function addLog(profileId, message, type = 'info', timestamp = null, level = null, skipNotify = false) {
            const session = getOrCreateSession(profileId);
            
            if (!timestamp) {
                timestamp = formatTimestamp(new Date());
            }

            const logEntry = { 
                message, 
                type, 
                timestamp: formatTimestamp(timestamp), 
                level: level || type.toUpperCase(),
                id: Date.now() + Math.random()
            };

            session.logs.push(logEntry);

            if (session.logs.length > MAX_LOGS) {
                session.logs.shift();
            }

            if (!skipNotify) {
                notifyListeners(profileId, logEntry);
            }
        }

        function notifyListeners(profileId, logEntry) {
            const session = profileSessions.get(profileId);
            if (!session) return;

            session.listeners.forEach(listener => {
                try {
                    listener(logEntry.message, logEntry.type, logEntry.timestamp, logEntry.level);
                } catch (e) {
                    console.error('Error in terminal listener:', e);
                }
            });
        }

        async function loadAllLogs(profileId) {
            try {
                const session = getOrCreateSession(profileId);
                console.log(`Loading all logs for profile ${profileId}...`);
                
                const allLogs = await rpc('/icomply/logs/all', { 
                    profile_id: profileId,
                    limit: null
                });
                
                session.logs.length = 0;
                
                console.log(`Received ${allLogs.length} logs for profile ${profileId}`);
                
                allLogs.forEach(log => {
                    addLog(
                        profileId,
                        log.message,
                        log.type,
                        log.timestamp,
                        log.level,
                        true
                    );
                });

                if (session.isInitialized) {
                    addLog(profileId, `Loaded ${allLogs.length} logs from file system`, 'success');
                } else {
                    console.log(`Initial load for profile ${profileId}: ${allLogs.length} logs loaded`);
                }

            } catch (error) {
                console.error(`Error loading logs for profile ${profileId}:`, error);
                addLog(profileId, `Error loading logs: ${error.message}`, 'error');
            }
        }

        async function pollNewLogs(profileId) {
            const session = getOrCreateSession(profileId);
            
            try {
                const result = await rpc('/icomply/logs/poll', { 
                    profile_id: profileId,
                    last_position: session.filePosition 
                });

                if (result.logs && result.logs.length > 0) {
                    const levelMapping = {
                        'DEBUG': 'info',
                        'INFO': 'info',
                        'WARNING': 'warning', 
                        'ERROR': 'error',
                        'CRITICAL': 'error',
                    };

                    result.logs.forEach(log => {
                        const type = levelMapping[log.level] || 'info';
                        addLog(
                            profileId,
                            log.message,
                            type,
                            log.timestamp,
                            log.level,
                            session.isPaused
                        );
                    });

                    if (!session.isPaused) {
                        console.log(`Polled ${result.logs.length} new logs for profile ${profileId}`);
                    }
                }

                session.filePosition = result.position;

            } catch (error) {
                console.error(`Error polling logs for profile ${profileId}:`, error);
                if (session.isInitialized && !session.isPaused) {
                    addLog(profileId, `Polling error: ${error.message}`, 'warning');
                }
            }
        }

        async function initialize(profileId) {
            try {
                const session = getOrCreateSession(profileId);
                
                // Load profile information
                const profileInfo = await rpc('/icomply/logs/profile/info', { 
                    profile_id: profileId 
                });
                
                if (profileInfo.error) {
                    console.error('Profile not found:', profileId);
                    addLog(profileId, 'Error: Profile not found', 'error');
                    return;
                }
                
                session.profileInfo = profileInfo;
                
                // Load all logs
                await loadAllLogs(profileId);
                
                // Get initial file position
                try {
                    const result = await rpc('/icomply/logs/poll', { 
                        profile_id: profileId,
                        last_position: 0 
                    });
                    session.filePosition = result.position;
                    console.log(`Initial file position for profile ${profileId}: ${session.filePosition}`);
                } catch (e) {
                    console.warn(`Could not set initial file position for profile ${profileId}:`, e);
                }
                
                // Subscribe to bus channel
                const channel = `icomply_logs_realtime_${profileId}`;
                bus_service.addChannel(channel);

                bus_service.addEventListener('notification', (ev) => {
                    const notifications = ev.detail;
                    if (Array.isArray(notifications)) {
                        notifications.forEach((notification) => {
                            if (notification.type === 'new_logs' && 
                                notification.payload && 
                                notification.payload.profile_id === profileId) {
                                const payload = notification.payload;
                                if (payload.logs && Array.isArray(payload.logs)) {
                                    payload.logs.forEach(log => {
                                        addLog(
                                            profileId,
                                            log.message, 
                                            log.type, 
                                            log.timestamp, 
                                            log.level
                                        );
                                    });
                                    session.filePosition = payload.position;
                                }
                            }
                        });
                    }
                });

                // Start polling
                session.pollInterval = setInterval(() => {
                    pollNewLogs(profileId);
                }, 2000);

                session.isInitialized = true;
                addLog(profileId, `Terminal initialized for ${profileInfo.name}`, 'success');

            } catch (error) {
                console.error(`Error initializing profile ${profileId}:`, error);
                addLog(profileId, `Initialization error: ${error.message}`, 'error');
            }
        }

        function cleanup(profileId) {
            const session = profileSessions.get(profileId);
            if (session && session.pollInterval) {
                clearInterval(session.pollInterval);
                session.pollInterval = null;
            }
        }

        function cleanupAll() {
            profileSessions.forEach((session, profileId) => {
                cleanup(profileId);
            });
        }

        window.addEventListener('beforeunload', cleanupAll);

        return {
            async initProfile(profileId) {
                await initialize(profileId);
            },

            addLog(profileId, message, type, timestamp, level) {
                addLog(profileId, message, type, timestamp, level);
            },

            onLog(profileId, listener) {
                const session = getOrCreateSession(profileId);
                session.listeners.add(listener);
                return () => {
                    session.listeners.delete(listener);
                };
            },

            getLogs(profileId) {
                const session = profileSessions.get(profileId);
                return session ? [...session.logs] : [];
            },

            clearLogs(profileId) {
                const session = profileSessions.get(profileId);
                if (session) {
                    session.logs.length = 0;
                    addLog(profileId, 'Local logs cleared', 'info');
                }
            },

            

            async reloadAllLogs(profileId) {
                await loadAllLogs(profileId);
            },
            async clearLogFile(profileId) {
                try {
                    const result = await rpc('/icomply/logs/clear_file', { 
                        profile_id: profileId 
                    });
                    
                    if (result.success) {
                        const session = getOrCreateSession(profileId);
                        session.logs.length = 0;
                        session.filePosition = 0;
                        addLog(profileId, result.message, 'success');
                    } else {
                        addLog(profileId, result.message, 'error');
                    }
                    
                    return result;
                } catch (error) {
                    console.error('Error clearing log file:', error);
                    addLog(profileId, `Error clearing file: ${error.message}`, 'error');
                    return { success: false, message: error.message };
                }
            },

            async refreshRecentLogs(profileId, limit = 100) {
                try {
                    const session = getOrCreateSession(profileId);
                    const recentLogs = await rpc('/icomply/logs/recent', { 
                        profile_id: profileId,
                        limit: limit,
                        get_all: false 
                    });
                    
                    session.logs.length = 0;
                    
                    recentLogs.forEach(log => {
                        addLog(
                            profileId,
                            log.message,
                            log.type,
                            log.timestamp,
                            log.level,
                            true
                        );
                    });

                    addLog(profileId, `Refreshed with ${recentLogs.length} recent logs`, 'info');
                } catch (error) {
                    console.error('Error loading recent logs:', error);
                    addLog(profileId, `Error loading logs: ${error.message}`, 'error');
                }
            },

            async pollNow(profileId) {
                await pollNewLogs(profileId);
            },

            async getStats(profileId) {
                try {
                    return await rpc('/icomply/logs/stats', { profile_id: profileId });
                } catch (error) {
                    console.error('Error getting log stats:', error);
                    return {};
                }
            },

            

            pausePolling(profileId) {
                const session = profileSessions.get(profileId);
                if (session) {
                    session.isPaused = true;
                    addLog(profileId, 'Display paused - logs still accumulating', 'info');
                }
            },

            resumePolling(profileId) {
                const session = profileSessions.get(profileId);
                if (session) {
                    session.isPaused = false;
                    addLog(profileId, 'Display resumed - showing accumulated logs', 'info');
                    
                    // Notify about accumulated logs
                    session.listeners.forEach(listener => {
                        session.logs.slice(-50).forEach(log => {
                            try {
                                listener(log.message, log.type, log.timestamp, log.level);
                            } catch (e) {
                                console.error('Error in terminal listener:', e);
                            }
                        });
                    });
                }
            },

            isPaused(profileId) {
                const session = profileSessions.get(profileId);
                return session ? session.isPaused : false;
            },

            getFilePosition(profileId) {
                const session = profileSessions.get(profileId);
                return session ? session.filePosition : 0;
            },

            resetFilePosition(profileId) {
                const session = profileSessions.get(profileId);
                if (session) {
                    session.filePosition = 0;
                    addLog(profileId, 'File position reset', 'info');
                }
            },

            getProfileInfo(profileId) {
                const session = profileSessions.get(profileId);
                return session ? session.profileInfo : null;
            },

            cleanup(profileId) {
                cleanup(profileId);
            },

            cleanupAll,

            isInitialized(profileId) {
                const session = profileSessions.get(profileId);
                return session ? session.isInitialized : false;
            },

            getStatus(profileId) {
                const session = profileSessions.get(profileId);
                if (!session) {
                    return {
                        initialized: false,
                        paused: false,
                        filePosition: 0,
                        logCount: 0,
                        listenerCount: 0,
                        maxLogs: MAX_LOGS,
                        profileInfo: null,
                    };
                }

                return {
                    initialized: session.isInitialized,
                    paused: session.isPaused,
                    filePosition: session.filePosition,
                    logCount: session.logs.length,
                    listenerCount: session.listeners.size,
                    maxLogs: MAX_LOGS,
                    profileInfo: session.profileInfo,
                };
            }
        };
    }
};

registry.category("services").add("icomply_terminal", icomplyTerminalService);