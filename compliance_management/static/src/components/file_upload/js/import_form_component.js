/** @odoo-module **/
import { Component, useState, useRef, onMounted, onWillUnmount } from "@odoo/owl";
import { useService } from "@web/core/utils/hooks";
import { _t } from "@web/core/l10n/translation";
import { useDebounced } from "@web/core/utils/timing";
import { browser } from "@web/core/browser/browser";
import { TerminalComponent } from "./terminal_component"; 

export class ImportFormComponent extends Component {
    setup() {
        console.log("ImportFormComponent setup starting...");
        // Initialize hooks
        this.state = useState({
            selectedFile: null,
            selectedModel: null,
            showWarning: false,
            showTemplates: false,
            isUploading: false,
            uploadProgress: 0,
            uploadSpeed: '',
            uploadedSize: '',
            totalSize: '',
            models: [],
            filteredModels: [],
            searchTerm: '',
            selectedTemplates: [],
            showModal: false,
            modalType: 'success',
            modalTitle: '',
            modalMessage: '',
            processingFiles: false,
            deleteMode: false,
            tableColumns: [],
            selectedUniqueIdentifier: null,
            loadingColumns: false,
            errorMessage: null // Add error message state
        });

        // Get DOM references
        this.fileInput = useRef("fileInput");
        this.fileNameRef = useRef("fileName");
        this.modelSelectorRef = useRef("modelSelector");
        this.modelSearchRef = useRef("modelSearch");
        this.progressRef = useRef("uploadProgress");
        this.progressTextRef = useRef("progressText");
        this.spinnerRef = useRef("spinner");
        this.modalRef = useRef("uploadModal");
        this.terminalContainerRef = useRef("terminalContainer");
        this.selectAllTemplatesRef = useRef("selectAllTemplates");

        // Safely get services with fallbacks
        this.initServices();

        // Setup debounced search
        this.debouncedSearch = useDebounced(this.searchModels.bind(this), 300);

        // Setup event handlers
        onMounted(() => {
            // Log initialization
            if (this.terminal) {
                this.terminal.addLog("CSV Import form initialized", "info");
            } else {
                console.log("CSV Import form initialized (terminal service not available)");
            }

            // Load models
            this.loadModels();
        });

        onWillUnmount(() => {
            // Abort upload if in progress
            if (this.uploader && this.state.isUploading) {
                this.uploader.abort();
            }

            // Clear any intervals
            if (this.statusCheckInterval) {
                clearInterval(this.statusCheckInterval);
                this.statusCheckInterval = null;
            }
        });
        console.log("ImportFormComponent setup complete");
    }

    /**
     * Safely initialize services with fallbacks
     */
    initServices() {
        try {
            // Basic required services
            this.rpc = useService("rpc");
            this.notification = useService("notification");

            // Optional services with fallbacks
            try {
                this.terminal = useService("terminal");
            } catch (e) {
                console.warn("Terminal service not available, using fallback");
                this.terminal = {
                    addLog: (message, type) => {
                        console.log(`[${type.toUpperCase()}] ${message}`);
                    },
                    onLog: () => () => { },
                    getLogs: () => [],
                    clearLogs: () => { },
                    sendLog: (message, type) => {
                        console.log(`[${type.toUpperCase()}] ${message}`);
                    }
                };
            }

            try {
                this.chunkedUploader = useService("chunkedUploader");
            } catch (e) {
                console.warn("ChunkedUploader service not available, using fallback");
                this.chunkedUploader = {
                    createUploader: () => ({
                        abort: () => { },
                        upload: () => {
                            this.state.errorMessage = "File upload service is not available. Please contact your administrator.";
                            return Promise.reject("Uploader not available");
                        }
                    })
                };
            }
        } catch (e) {
            console.error("Error initializing services:", e);
            this.state.errorMessage = "Failed to initialize required services. Please refresh the page or contact your administrator.";
        }
    }

    /**
     * Load available import models from the server
     */
    async loadModels() {
        try {
            console.log("Loading models...");
            this.state.errorMessage = null;

            // Show loading message
            if (this.terminal) {
                this.terminal.addLog("Loading available import models...", "info");
            }

            // Add a timeout to prevent hanging
            const modelPromise = this.rpc("/csv_import/get_import_models", { limit: 200 });
            const timeoutPromise = new Promise((_, reject) =>
                setTimeout(() => reject(new Error("Request timeout")), 10000)
            );

            const result = await Promise.race([modelPromise, timeoutPromise]);

            // Check if result contains error
            if (result.error) {
                throw new Error(result.error);
            }

            this.state.models = result.models || [];
            this.state.filteredModels = [...this.state.models];

            console.log(`Loaded ${this.state.models.length} models`);
            if (this.terminal) {
                this.terminal.addLog(`Loaded ${this.state.models.length} available models for import`, "info");
            }
        } catch (error) {
            console.error("Error loading models:", error);

            // More helpful error message
            const errorMsg = `Failed to load models: ${error.message || 'Unknown error'}. Check server logs for details.`;
            this.state.errorMessage = errorMsg;

            if (this.terminal) {
                this.terminal.addLog(errorMsg, "error");
            }
            if (this.notification) {
                this.notification.add(_t("Failed to load import models"), {
                    type: "danger",
                });
            }

            // Set empty models to prevent UI errors
            this.state.models = [];
            this.state.filteredModels = [];
        }
    }

    /**
     * Search models based on term
     */
    async searchModels() {
        const term = this.state.searchTerm;

        if (!term) {
            this.state.filteredModels = [...this.state.models];
            return;
        }

        try {
            const result = await this.rpc("/csv_import/get_import_models", {
                search_term: term,
                limit: 200
            });

            this.state.filteredModels = result.models || [];
        } catch (error) {
            console.error("Error searching models:", error);
            if (this.notification) {
                this.notification.add(_t("Search failed"), {
                    type: "warning",
                });
            }
        }
    }

    /**
     * Toggle warning section visibility
     */
    toggleWarning() {
        this.state.showWarning = !this.state.showWarning;
    }

    /**
     * Toggle templates section visibility
     */
    toggleTemplates() {
        this.state.showTemplates = !this.state.showTemplates;
    }

    /**
     * Handle select all templates checkbox
     * @param {Event} ev - Change event
     */
    onSelectAllTemplates(ev) {
        const isChecked = ev.target.checked;
        this.state.selectedTemplates = isChecked
            ? this.state.models.map(model => model.id)
            : [];
    }

    /**
     * Handle template checkbox change
     * @param {number} modelId - Model ID
     * @param {Event} ev - Change event
     */
    onTemplateCheckboxChange(modelId, ev) {
        const isChecked = ev.target.checked;

        if (isChecked) {
            if (!this.state.selectedTemplates.includes(modelId)) {
                this.state.selectedTemplates.push(modelId);
            }
        } else {
            this.state.selectedTemplates = this.state.selectedTemplates.filter(id => id !== modelId);
        }

        // Update "select all" checkbox state
        if (this.selectAllTemplatesRef.el) {
            this.selectAllTemplatesRef.el.checked =
                this.state.selectedTemplates.length === this.state.models.length;
        }
    }

    /**
     * Handle file input change
     * @param {Event} ev - Change event
     */
    onFileChanged(ev) {
        const file = ev.target.files[0];

        if (file) {
            this.state.selectedFile = file;

            // Show file name
            if (this.fileNameRef.el) {
                this.fileNameRef.el.textContent = file.name;
            }

            if (this.terminal) {
                this.terminal.addLog(`Selected file: ${file.name} (${this.formatBytes(file.size)})`, "info");
            }

            // Validate file type
            const validExtensions = ['.csv', '.xlsx', '.xls'];
            const fileExt = file.name.substr(file.name.lastIndexOf('.')).toLowerCase();

            if (!validExtensions.includes(fileExt)) {
                if (this.terminal) {
                    this.terminal.addLog(`Warning: File type ${fileExt} may not be supported. Please use CSV or Excel files.`, "warning");
                }
            }

            // Validate file size
            if (file.size > 2 * 1024 * 1024 * 1024) { // 2GB
                if (this.terminal) {
                    this.terminal.addLog("Error: File size exceeds 2GB limit", "error");
                }
                this.state.selectedFile = null;
                if (this.fileNameRef.el) {
                    this.fileNameRef.el.textContent = _t("Please select a smaller file");
                }

                // Reset file input
                if (this.fileInput.el) {
                    this.fileInput.el.value = "";
                }
            }
        } else {
            this.state.selectedFile = null;
            if (this.fileNameRef.el) {
                this.fileNameRef.el.textContent = _t("No file selected");
            }
        }
    }

    /**
     * Handle model selector change
     * @param {Event} ev - Change event
     */
    onModelChanged(ev) {
        const modelId = parseInt(ev.target.value, 10);
        
        if (isNaN(modelId) || modelId <= 0) {
            console.log("No valid model selected");
            this.state.selectedModel = null;
            return;
        }
        
        // First check in filteredModels (what's currently in the dropdown)
        let model = this.state.filteredModels.find(m => m.id === modelId);
        
        // If not found in filtered models, check in the full models array
        if (!model) {
            console.log(`Model ${modelId} not found in filtered models, checking full list`);
            model = this.state.models.find(m => m.id === modelId);
        }
        
        if (model) {
            console.log("Selected model:", model);
            
            // Add a description if missing
            if (!model.description) {
                console.log("Adding default description for model");
                model = { 
                    ...model, 
                    description: `Import data into ${model.name} (${model.model_name}). Please ensure your file has the required fields.` 
                };
            }
            
            // Update selected model in state - create a new object to ensure reactivity
            this.state.selectedModel = { ...model };
            
            // Log the selection
            if (this.terminal) {
                this.terminal.addLog(`Selected model: ${model.name}`, "info");
            }

            if (this.state.deleteMode) {
                this.fetchTableColumns(model.id);
            }
            
            // Force UI update by setting a key property
            this.state.selectedModelKey = Date.now();
        } else {
            console.error(`Model with ID ${modelId} not found in any model list`);
            this.state.selectedModel = null;
        }
    }

    /**
     * Handle model search input
     * @param {Event} ev - Input event
     */
    onModelSearch(ev) {
        this.state.searchTerm = ev.target.value;
        this.debouncedSearch();
    }

    /**
     * Handle download templates button click
     */
    async onDownloadTemplates() {
        if (this.state.selectedTemplates.length === 0) {
            if (this.terminal) {
                this.terminal.addLog("Please select at least one template to download", "warning");
            }
            return;
        }

        // Set processing state
        this.state.processingFiles = true;

        try {
            // Download templates sequentially
            for (const modelId of this.state.selectedTemplates) {
                const model = this.state.models.find(m => m.id === modelId);
                if (!model) continue;

                if (this.terminal) {
                    this.terminal.addLog(`Downloading template for ${model.name}...`, "info");
                }

                // Create a download link
                const link = document.createElement('a');
                link.href = `/csv_import/download_template/${modelId}`;
                link.download = `${model.name}_template.xlsx`;
                link.style.display = 'none';
                document.body.appendChild(link);
                link.click();
                document.body.removeChild(link);

                // Wait a bit between downloads
                await new Promise(resolve => setTimeout(resolve, 1000));

                if (this.terminal) {
                    this.terminal.addLog(`Downloaded template for ${model.name}`, "success");
                }
            }

            if (this.terminal) {
                this.terminal.addLog("All selected templates downloaded successfully", "success");
            }
        } catch (error) {
            console.error("Error downloading templates:", error);
            if (this.terminal) {
                this.terminal.addLog(`Error downloading templates: ${error.message}`, "error");
            }
        } finally {
            this.state.processingFiles = false;
        }
    }

    /**
     * Handle upload button click
     * @param {Event} ev - Click event
     */
    async onUpload(ev) {
        ev.preventDefault();

        // Validate inputs
        if (!this.state.selectedFile) {
            this.showMessage("error", _t("Upload Failed"), _t("Please select a file to upload."));
            return;
        }

        if (!this.state.selectedModel) {
            this.showMessage("error", _t("Upload Failed"), _t("Please select a target model."));
            return;
        }

        if (this.state.deleteMode && !this.state.selectedUniqueIdentifier) {
            this.showMessage("error", _t("Upload Failed"), _t("Please select a unique identifier field for delete mode."));
            return;
        }

        // Show progress container
        this.state.isUploading = true;
        this.state.uploadProgress = 0;

        // Check if chunkedUploader service is available
        if (!this.chunkedUploader || !this.chunkedUploader.createUploader) {
            this.showMessage("error", _t("Upload Failed"), _t("Upload service is not available."));
            this.state.isUploading = false;
            return;
        }

        // Create uploader
        this.uploader = this.chunkedUploader.createUploader(this.state.selectedFile, {
            onLog: (message, type) => {
                if (this.terminal) {
                    this.terminal.addLog(message, type);
                }
            },
            onProgress: (progress) => {
                this.state.uploadProgress = parseFloat(progress.percentage);
                this.state.uploadSpeed = progress.speed;
                this.state.uploadedSize = progress.uploaded;
                this.state.totalSize = progress.total;
            }
        });

        const uploadParams = {
            modelId: this.state.selectedModel.id
        };
        
        if (this.state.deleteMode) {
            uploadParams.deleteMode = true;
            uploadParams.uniqueIdentifierField = this.state.selectedUniqueIdentifier;
        }

        try {
            // Start upload
            const result = await this.uploader.upload(uploadParams);
            // const result = await this.uploader.upload({
            //     modelId: this.state.selectedModel.id
            // });

            // Handle success
            // this.showMessage("success", _t("Upload Successful"),
            //     _t("Your file has been uploaded and is being processed"));

            // // Reset form
            // this.resetForm();
            this.onUploadSuccess(result);
        } catch (error) {
            // Handle error
            this.showMessage("error", _t("Upload Failed"), error.message);
        } finally {
            // Hide progress
            this.state.isUploading = false;
        }
    }

    /**
     * Handle file upload success with improved async handling
     * @param {Object} result - Upload result from server
     */
    onUploadSuccess(result) {
        if (result.mode === 'delete') {
            // For delete operations
            this.showMessage(
                "success", 
                _t("File Uploaded for Deletion"),
                _t("Your file has been uploaded and the deletion process has been queued. You can monitor progress in the logs below.")
            );
            
            // Add tracking information to show status
            if (this.terminal) {
                this.terminal.addLog(`Delete operation started for import ID: ${result.import_id}`, "info");
                this.terminal.addLog("The operation will continue in the background and you'll see progress updates here", "info");
                
                // Set an interval to check status (every 5 seconds)
                this.statusCheckInterval = setInterval(() => {
                    this.checkDeleteStatus(result.import_id);
                }, 5000);
                
                // Store the import ID for status checking
                this.currentDeleteImportId = result.import_id;
            }
        } else {
            // For regular imports
            this.showMessage(
                "success", 
                _t("Upload Successful"),
                _t("Your file has been uploaded and is being processed")
            );
        }
        
        // Reset form in both cases
        this.resetForm();
    }

    async fetchTableColumns(modelId) {
        if (!modelId) return;
        
        this.state.loadingColumns = true;
        this.state.tableColumns = [];
        
        try {
            if (this.terminal) {
                this.terminal.addLog(`Fetching columns for table ID ${modelId}...`, "info");
            }
            
            const result = await this.rpc("/csv_import/get_table_columns", {
                model_id: modelId
            });
            
            if (result.error) {
                throw new Error(result.error);
            }
            
            this.state.tableColumns = result.columns || [];
            
            if (this.terminal) {
                this.terminal.addLog(`Loaded ${this.state.tableColumns.length} columns for selected table`, "success");
            }
        } catch (error) {
            console.error("Error fetching table columns:", error);
            this.state.errorMessage = `Failed to fetch table columns: ${error.message}`;
            
            if (this.terminal) {
                this.terminal.addLog(`Error fetching table columns: ${error.message}`, "error");
            }
        } finally {
            this.state.loadingColumns = false;
        }
    }

    onDeleteModeChange(ev) {
        this.state.deleteMode = ev.target.checked;
        
        // Reset unique identifier when toggling
        this.state.selectedUniqueIdentifier = null;
        
        if (this.state.deleteMode && this.state.selectedModel) {
            // Fetch columns when delete mode is enabled and we have a model
            this.fetchTableColumns(this.state.selectedModel.id);
        }
        
        if (this.terminal) {
            if (this.state.deleteMode) {
                this.terminal.addLog("Delete mode enabled - records with matching identifiers will be deleted", "warning");
            } else {
                this.terminal.addLog("Delete mode disabled", "info");
            }
        }
    }

    onUniqueIdentifierChange(ev) {
        this.state.selectedUniqueIdentifier = ev.target.value;
        
        if (this.terminal) {
            const field = this.state.tableColumns.find(col => col.name === this.state.selectedUniqueIdentifier);
            if (field) {
                this.terminal.addLog(`Selected "${field.string}" as unique identifier for deletion`, "info");
            }
        }
    }

    /**
     * Handle file upload success with improved async handling
     * @param {Object} result - Upload result from server
     */
    onUploadSuccess(result) {
        if (result.mode === 'delete') {
            // For delete operations
            this.showMessage(
                "success", 
                _t("File Uploaded for Deletion"),
                _t("Your file has been uploaded and the deletion process has been queued. You can monitor progress in the logs below.")
            );
            
            // Add tracking information to show status
            if (this.terminal) {
                this.terminal.addLog(`Delete operation started for import ID: ${result.import_id}`, "info");
                this.terminal.addLog("The operation will continue in the background and you'll see progress updates here", "info");
                
                // Set an interval to check status (every 5 seconds)
                this.statusCheckInterval = setInterval(() => {
                    this.checkDeleteStatus(result.import_id);
                }, 5000);
                
                // Store the import ID for status checking
                this.currentDeleteImportId = result.import_id;
            }
        } else {
            // For regular imports
            this.showMessage(
                "success", 
                _t("Upload Successful"),
                _t("Your file has been uploaded and is being processed")
            );
        }
        
        // Reset form in both cases
        this.resetForm();
    }

    async fetchTableColumns(modelId) {
        if (!modelId) return;
        
        this.state.loadingColumns = true;
        this.state.tableColumns = [];
        
        try {
            if (this.terminal) {
                this.terminal.addLog(`Fetching columns for table ID ${modelId}...`, "info");
            }
            
            const result = await this.rpc("/csv_import/get_table_columns", {
                model_id: modelId
            });
            
            if (result.error) {
                throw new Error(result.error);
            }
            
            this.state.tableColumns = result.columns || [];
            
            if (this.terminal) {
                this.terminal.addLog(`Loaded ${this.state.tableColumns.length} columns for selected table`, "success");
            }
        } catch (error) {
            console.error("Error fetching table columns:", error);
            this.state.errorMessage = `Failed to fetch table columns: ${error.message}`;
            
            if (this.terminal) {
                this.terminal.addLog(`Error fetching table columns: ${error.message}`, "error");
            }
        } finally {
            this.state.loadingColumns = false;
        }
    }

    onDeleteModeChange(ev) {
        this.state.deleteMode = ev.target.checked;
        
        // Reset unique identifier when toggling
        this.state.selectedUniqueIdentifier = null;
        
        if (this.state.deleteMode && this.state.selectedModel) {
            // Fetch columns when delete mode is enabled and we have a model
            this.fetchTableColumns(this.state.selectedModel.id);
        }
        
        if (this.terminal) {
            if (this.state.deleteMode) {
                this.terminal.addLog("Delete mode enabled - records with matching identifiers will be deleted", "warning");
            } else {
                this.terminal.addLog("Delete mode disabled", "info");
            }
        }
    }

    onUniqueIdentifierChange(ev) {
        this.state.selectedUniqueIdentifier = ev.target.value;
        
        if (this.terminal) {
            const field = this.state.tableColumns.find(col => col.name === this.state.selectedUniqueIdentifier);
            if (field) {
                this.terminal.addLog(`Selected "${field.string}" as unique identifier for deletion`, "info");
            }
        }
    }

    /**
     * Check the status of a delete operation
     * @param {number} importId - The import ID to check
     */
    async checkDeleteStatus(importId) {
        if (!importId) return;
        
        try {
            const result = await this.rpc("/web/dataset/call_kw", {
                model: "import.log",
                method: "read",
                args: [[importId], ["delete_progress", "status"]],
                kwargs: {}
            });
            
            if (!result || !result.length) {
                // Import not found, stop checking
                clearInterval(this.statusCheckInterval);
                this.statusCheckInterval = null;
                return;
            }
            
            const importData = result[0];
            
            // Check if we have delete progress
            if (importData.delete_progress) {
                try {
                    const progress = JSON.parse(importData.delete_progress);
                    
                    // Show progress based on status
                    if (progress.status === 'completed') {
                        if (this.terminal) {
                            this.terminal.addLog(`Delete operation completed: ${progress.deleted} records deleted, ${progress.failed} failed`, "success");
                        }
                        
                        // Stop checking
                        clearInterval(this.statusCheckInterval);
                        this.statusCheckInterval = null;
                        
                    } else if (progress.status === 'failed') {
                        if (this.terminal) {
                            this.terminal.addLog(`Delete operation failed: ${progress.error_message || 'Unknown error'}`, "error");
                        }
                        
                        // Stop checking
                        clearInterval(this.statusCheckInterval);
                        this.statusCheckInterval = null;
                        
                    } else if (progress.status === 'in_progress') {
                        // Calculate progress percentage
                        const totalValues = progress.total || 0;
                        const processedValues = progress.processed || 0;
                        const percentage = totalValues > 0 ? Math.round((processedValues / totalValues) * 100) : 0;
                        
                        // Show progress in terminal (but not too frequently to avoid spam)
                        if (percentage % 10 === 0 || percentage > 95) {
                            if (this.terminal) {
                                this.terminal.addLog(`Delete progress: ${percentage}% (${processedValues}/${totalValues})`, "info");
                            }
                        }
                    }
                } catch (e) {
                    console.error("Error parsing delete progress:", e);
                }
            }
            
            // Check if import status is completed or failed
            if (importData.status === 'completed' || importData.status === 'failed') {
                // Stop checking
                clearInterval(this.statusCheckInterval);
                this.statusCheckInterval = null;
            }
            
        } catch (error) {
            console.error("Error checking delete status:", error);
        }
    }

    /**
     * Reset form after upload
     */
    resetForm() {
        // Reset file input
        if (this.fileInput.el) {
            this.fileInput.el.value = "";
        }

        if (this.fileNameRef.el) {
            this.fileNameRef.el.textContent = _t("No file selected");
        }

        this.state.selectedFile = null;

        // Keep model selection
    }

    /**
     * Show message modal
     * @param {string} type - Message type (success, error)
     * @param {string} title - Modal title
     * @param {string} message - Modal message
     */
    showMessage(type, title, message) {
        this.state.modalType = type;
        this.state.modalTitle = title;
        this.state.modalMessage = message;
        this.state.showModal = true;
    }

    /**
     * Close modal
     */
    closeModal() {
        this.state.showModal = false;
    }

    /**
     * Format bytes to human-readable size
     * @param {number} bytes - Size in bytes
     * @param {number} [decimals=2] - Number of decimal places
     * @returns {string} - Formatted size with units
     */
    formatBytes(bytes, decimals = 2) {
        if (bytes === 0) return '0 Bytes';
        const k = 1024;
        const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB'];
        const i = Math.floor(Math.log(bytes) / Math.log(k));
        return parseFloat((bytes / Math.pow(k, i)).toFixed(decimals)) + ' ' + sizes[i];
    }
}

// Define component properties
ImportFormComponent.template = "compliance_management.ImportForm";
ImportFormComponent.components = { TerminalComponent }; 
ImportFormComponent.props = {
    // action: { type: String, optional: true },
    action: { type: [String, Object], optional: true },
    actionId: { type: Number, optional: true },
    className: { type: String, optional: true }
};
