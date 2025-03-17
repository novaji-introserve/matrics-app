/** @odoo-module **/
import { browser } from "@web/core/browser/browser";
import { registry } from "@web/core/registry";
import { session } from "@web/session";

/**
 * ChunkedUploader - Service for handling file uploads in chunks
 * 
 * Provides methods for uploading large files in chunks with progress tracking,
 * error handling, and automatic retries.
 */
class ChunkedUploader {
    /**
     * Initialize a new uploader for a file
     * 
     * @param {File} file - The file to upload
     * @param {Object} options - Configuration options
     */
    constructor(file, options = {}) {
        this.file = file;
        this.options = {
            chunkSize: 5 * 1024 * 1024, // 5MB chunks
            maxRetries: 3,
            retryDelay: 2000, // 2 seconds
            onLog: null,
            onProgress: null,
            onChunkProgress: null,
            ...options
        };

        this.chunkSize = this.options.chunkSize;
        this.chunkCount = Math.ceil(file.size / this.chunkSize);
        this.currentChunk = 0;
        this.fileId = this._generateFileId();
        this.aborted = false;
        this.retries = {};

        // Log initialization
        this._logMessage(`Preparing to upload ${file.name} (${this._formatBytes(file.size)}) in ${this.chunkCount} chunks`);
    }

    /**
     * Generate a unique file ID for tracking chunks
     * @private
     * @returns {string} - Unique ID
     */
    _generateFileId() {
        return Date.now() + '-' + Math.random().toString(36).substring(2, 15);
    }

    /**
     * Convert bytes to human-readable format
     * @private
     * @param {number} bytes - Size in bytes
     * @param {number} [decimals=2] - Number of decimal places
     * @returns {string} - Formatted size with units
     */
    _formatBytes(bytes, decimals = 2) {
        if (bytes === 0) return '0 Bytes';
        const k = 1024;
        const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB'];
        const i = Math.floor(Math.log(bytes) / Math.log(k));
        return parseFloat((bytes / Math.pow(k, i)).toFixed(decimals)) + ' ' + sizes[i];
    }

    /**
     * Log a message through the callback if provided
     * @private
     * @param {string} message - The message to log
     * @param {string} [type='info'] - Message type (info, success, warning, error)
     */
    _logMessage(message, type = 'info') {
        if (typeof this.options.onLog === 'function') {
            this.options.onLog(message, type);
        }
        console.log(`[${type.toUpperCase()}] ${message}`);
    }

    /**
     * Upload a single chunk
     * @private
     * @param {Blob} chunk - The chunk data to upload
     * @param {number} chunkNumber - Index of the chunk
     * @param {Object} metadata - Additional data to send
     * @returns {Promise} - Promise resolving to the server response
     */
    async _uploadChunk(chunk, chunkNumber, metadata) {
        if (this.aborted) {
            throw new Error('Upload aborted');
        }

        this._logMessage(`Uploading chunk ${chunkNumber + 1} of ${this.chunkCount}`, 'info');

        const formData = new FormData();
        // Add a filename to help with MIME type detection
        formData.append('chunk', chunk, 'chunk.bin');

        const headers = {
            'X-Chunk-Number': chunkNumber,
            'X-Total-Chunks': this.chunkCount,
            'X-File-Id': this.fileId,
            'X-Original-Filename': this.file.name,
            'X-Model-Id': metadata.modelId,
        };

        try {
            // Create an XMLHttpRequest to track progress
            return new Promise((resolve, reject) => {
                const xhr = new XMLHttpRequest();
                xhr.open('POST', '/csv_import/upload_chunk', true);

                // Set headers
                Object.keys(headers).forEach(key => {
                    xhr.setRequestHeader(key, headers[key]);
                });

                // Add CSRF token from session - safely get the token
                let csrf_token = null;
                // Try to get from session first (Odoo 16 approach)
                if (session && session.csrf_token) {
                    csrf_token = session.csrf_token;
                }
                // Fallback to browser cookies if available
                else if (browser && browser.cookie && typeof browser.cookie.get === 'function') {
                    csrf_token = browser.cookie.get('csrf_token');
                }
                // Final fallback - try to get from document
                else if (document.querySelector('meta[name="csrf-token"]')) {
                    csrf_token = document.querySelector('meta[name="csrf-token"]').getAttribute('content');
                }

                if (csrf_token) {
                    xhr.setRequestHeader('X-CSRFToken', csrf_token);
                } else {
                    console.warn('CSRF token not found - request may fail');
                }

                // Track progress
                xhr.upload.addEventListener('progress', (e) => {
                    if (e.lengthComputable && typeof this.options.onChunkProgress === 'function') {
                        const percentComplete = Math.round((e.loaded / e.total) * 100);
                        this.options.onChunkProgress(chunkNumber, percentComplete);
                    }
                });

                // Handle response
                xhr.onload = () => {
                    if (xhr.status >= 200 && xhr.status < 300) {
                        let result;
                        try {
                            result = JSON.parse(xhr.responseText);
                        } catch (e) {
                            result = { status: 'success' };
                        }
                        this._logMessage(`Successfully uploaded chunk ${chunkNumber + 1}/${this.chunkCount}`, 'success');
                        resolve(result);
                    } else {
                        let errorMsg = 'Upload error: ';

                        try {
                            const response = JSON.parse(xhr.responseText);
                            errorMsg += response.error || xhr.statusText;
                        } catch (e) {
                            errorMsg += xhr.statusText;
                        }

                        this._logMessage(errorMsg, 'error');
                        reject(new Error(errorMsg));
                    }
                };

                // Handle network errors
                xhr.onerror = () => {
                    this._logMessage('Network error during upload', 'error');
                    reject(new Error('Network error during upload'));
                };

                // Handle timeout
                xhr.ontimeout = () => {
                    this._logMessage('Request timed out', 'error');
                    reject(new Error('Request timed out'));
                };

                // Send the request
                xhr.send(formData);
            });
        } catch (error) {
            this._logMessage(`Upload error: ${error.message}`, 'error');
            throw error;
        }
    }

    /**
     * Try to upload a chunk with retries
     * @private
     * @param {Blob} chunk - The chunk data to upload
     * @param {number} chunkNumber - Index of the chunk
     * @param {Object} metadata - Additional data to send
     * @returns {Promise} - Promise resolving to the server response
     */
    async _uploadChunkWithRetry(chunk, chunkNumber, metadata) {
        this.retries[chunkNumber] = this.retries[chunkNumber] || 0;

        try {
            return await this._uploadChunk(chunk, chunkNumber, metadata);
        } catch (error) {
            if (this.retries[chunkNumber] < this.options.maxRetries) {
                this.retries[chunkNumber]++;
                this._logMessage(`Retrying chunk ${chunkNumber + 1} (attempt ${this.retries[chunkNumber]})`, 'warning');

                // Wait before retrying
                await new Promise(resolve => setTimeout(resolve, this.options.retryDelay));

                // Try again
                return this._uploadChunkWithRetry(chunk, chunkNumber, metadata);
            }

            // Max retries reached, propagate the error
            throw error;
        }
    }

    /**
     * Abort the upload
     */
    abort() {
        this.aborted = true;
        this._logMessage('Upload aborted', 'warning');
    }

    /**
     * Start the upload process
     * 
     * @param {Object} metadata - Additional data to send with the upload
     * @param {number} metadata.modelId - ID of the target model for import
     * @returns {Promise} - Promise resolving when upload is complete
     */
    async upload(metadata) {
        if (!metadata || !metadata.modelId) {
            throw new Error('Model ID is required');
        }

        // File size validation
        if (this.file.size > 2 * 1024 * 1024 * 1024) { // 2GB
            this._logMessage('File size exceeds 2GB limit', 'error');
            throw new Error('File size exceeds 2GB limit');
        }

        // Start time for speed calculation
        const startTime = Date.now();
        let uploadedSize = 0;

        // Process chunks sequentially
        while (this.currentChunk < this.chunkCount && !this.aborted) {
            // Prepare the chunk
            const start = this.currentChunk * this.chunkSize;
            const end = Math.min(start + this.chunkSize, this.file.size);
            const chunk = this.file.slice(start, end);

            try {
                // Upload the chunk
                const result = await this._uploadChunkWithRetry(chunk, this.currentChunk, metadata);

                // Update progress
                this.currentChunk++;
                uploadedSize += chunk.size;

                // Calculate progress and speed
                const progress = (uploadedSize / this.file.size) * 100;
                const elapsedSeconds = (Date.now() - startTime) / 1000;
                const speed = uploadedSize / elapsedSeconds;

                // Report progress
                if (typeof this.options.onProgress === 'function') {
                    this.options.onProgress({
                        percentage: progress.toFixed(1),
                        speed: this._formatBytes(speed) + '/s',
                        uploaded: this._formatBytes(uploadedSize),
                        total: this._formatBytes(this.file.size)
                    });
                }

                // Check if this was the final chunk with import ID
                if (result && result.import_id) {
                    this._logMessage('Upload completed successfully', 'success');
                    return result;
                }
            } catch (error) {
                this._logMessage(`Upload failed: ${error.message}`, 'error');
                throw error;
            }
        }

        if (this.aborted) {
            throw new Error('Upload aborted');
        }

        this._logMessage('Upload completed successfully', 'success');
        return { status: 'success' };
    }
}

// Register as a service
export const chunkedUploaderService = {
    dependencies: ['notification'],

    start(env, { notification }) {
        return {
            /**
             * Create a new uploader instance
             * 
             * @param {File} file - The file to upload
             * @param {Object} options - Uploader options
             * @returns {ChunkedUploader} - A new uploader instance
             */
            createUploader(file, options = {}) {
                return new ChunkedUploader(file, options);
            }
        };
    }
};

registry.category("services").add("chunkedUploader", chunkedUploaderService);
