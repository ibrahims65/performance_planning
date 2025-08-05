#!/bin/bash

# Cloud Capacity Planning Script - Usage Zone Analysis
# Version: 1.35.3 - Implements robust, isolated memory parsing logic to fix modern OS calculation.
# Description: Analyzes system utilization, detects stale backups, and categorizes as Cold/Normal/Hot

set -euo pipefail

# --- UI & Animation Configuration ---
readonly GREEN='\033[0;32m'
readonly NC='\033[0m' # No Color

# Global Configuration
readonly SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly NFS_MOUNT="/mnt"
readonly CAPACITY_BASE_DIR="/mnt/capacity_planning"
readonly REPORTS_DIR="$CAPACITY_BASE_DIR/reports"
readonly TRENDS_DIR="$CAPACITY_BASE_DIR/weekly_summaries"
readonly LOG_DIR="$CAPACITY_BASE_DIR/logs"
readonly LOG_FILE="${LOG_DIR}/capacity_planning_$(date +%Y%m%d_%H%M%S).log"
readonly TRENDS_FILE="${TRENDS_DIR}/historical_trends.csv"
readonly TEMP_DIR="/tmp/capacity_planning_$$"
readonly RESULTS_FILE="${TEMP_DIR}/analysis_results.txt"
readonly SKIPPED_FILE="${TEMP_DIR}/skipped_hosts.txt"
readonly STALE_FILE="${TEMP_DIR}/stale_hosts.txt"
readonly SUSTAINED_STATUS_FILE="${TEMP_DIR}/sustained_status.txt"
readonly HISTORICAL_SUMMARY_FILE="${TEMP_DIR}/historical_summary.txt"

# Create necessary directories (done early as logs need LOG_DIR)
mkdir -p "$REPORTS_DIR" "$TRENDS_DIR" "$LOG_DIR" "$TEMP_DIR"

# Simplified Temperature Zone Thresholds
# COLD: Underutilized - CPU < 30% AND Memory < 40%
readonly COLD_CPU_MAX=30
readonly COLD_MEM_MAX=40

# NORMAL: Good utilization - CPU 30-70% AND Memory 40-80%
readonly NORMAL_CPU_MIN=30
readonly NORMAL_CPU_MAX=70
readonly NORMAL_MEM_MIN=40
readonly NORMAL_MEM_MAX=80

# HOT: Over-utilized - CPU > 70% OR Memory > 80%
readonly HOT_CPU_MIN=70
readonly HOT_MEM_MIN=80

# Exemption Thresholds
readonly HIGH_PEAK_THRESHOLD=75

# Stale Host Detection Threshold
readonly STALE_THRESHOLD_DAYS=7

# Default values for arguments
args_all=false
args_host=""
args_email=""
args_days=7
args_show_stale=false
VERBOSE_MODE=false

# Global variable to track if NFS was mounted by this script
NFS_MOUNTED_BY_SCRIPT=false

# --- Global Data Lookups ---
declare -A INSTANCE_COSTS
declare -A INSTANCE_NEXT_UP
declare -A INSTANCE_NEXT_DOWN

# Function for logging to stderr and file
log() {
    local level="$1"
    local message="$2"
    local log_line
    log_line="$(date '+%Y-%m-%d %H:%M:%S') [${level}] [$(basename "${BASH_SOURCE[0]}")][$$] ${message}"

    # Always write to the log file
    echo "$log_line" >> "$LOG_FILE"

    # Conditionally write to stderr (the console)
    if [[ "$level" != "DEBUG" || "$VERBOSE_MODE" == "true" ]]; then
        # During --all runs, suppress INFO logs from the console to keep the progress bar clean
        if [[ "$args_all" == "true" && "$level" == "INFO" ]]; then
            return
        fi
        echo "$log_line" >&2
    fi
}

# Trap for cleanup on exit
cleanup() {
    log "INFO" "Starting cleanup..."
    if [[ "$NFS_MOUNTED_BY_SCRIPT" == "true" ]]; then
        log "INFO" "Unmounting NFS share: ${NFS_MOUNT}"
        umount "${NFS_MOUNT}" || log "WARN" "Failed to unmount NFS share: ${NFS_MOUNT}. Manual unmount might be required."
    fi
    if [[ -d "$TEMP_DIR" ]]; then
        log "INFO" "Removing temporary directory: ${TEMP_DIR}"
        rm -rf "$TEMP_DIR"
    fi
    log "INFO" "Cleanup complete."
}
trap cleanup EXIT INT TERM

# Function to load pricing data from CSV
load_pricing_data() {
    local pricing_file="$1"
    if [[ ! -f "$pricing_file" ]]; then
        log "ERROR" "Pricing file not found: ${pricing_file}. Cost calculations will be disabled."
        return
    fi
    log "INFO" "Loading pricing data from ${pricing_file}..."
    # Skip header, use comma as delimiter
    while IFS=',' read -r instance_type cost_per_hour; do
        # Skip header line
        if [[ "$instance_type" == "instance_type" ]]; then continue; fi
        INSTANCE_COSTS["$instance_type"]="$cost_per_hour"
    done < "$pricing_file"
    log "INFO" "Loaded pricing for ${#INSTANCE_COSTS[@]} instance types."
}

# Function to load instance family data from CSV
load_instance_family_data() {
    local family_file="$1"
    if [[ ! -f "$family_file" ]]; then
        log "ERROR" "Instance family file not found: ${family_file}. Sizing recommendations will be limited."
        return
    fi
    log "INFO" "Loading instance family data from ${family_file}..."
    # Skip header, use comma as delimiter
    while IFS=',' read -r instance_type family size next_up next_down; do
        # Skip header line
        if [[ "$instance_type" == "instance_type" ]]; then continue; fi
        # Trim potential trailing carriage returns
        next_up=$(echo "$next_up" | tr -d '\r')
        next_down=$(echo "$next_down" | tr -d '\r')

        if [[ -n "$next_up" ]]; then
            INSTANCE_NEXT_UP["$instance_type"]="$next_up"
        fi
        if [[ -n "$next_down" ]]; then
            INSTANCE_NEXT_DOWN["$instance_type"]="$next_down"
        fi
    done < "$family_file"
    log "INFO" "Loaded family data for ${#INSTANCE_NEXT_UP[@]} 'next_up' and ${#INSTANCE_NEXT_DOWN[@]} 'next_down' configurations."
}

# Function to calculate the cost difference for a recommended change
calculate_cost_implication() {
    local current_instance_type="$1"
    local recommendation="$2"
    local target_instance_type=""

    # Determine the target instance type based on the recommendation
    if [[ "$recommendation" == "downsize" ]]; then
        target_instance_type=${INSTANCE_NEXT_DOWN[$current_instance_type]}
    elif [[ "$recommendation" == "upsize" ]]; then
        target_instance_type=${INSTANCE_NEXT_UP[$current_instance_type]}
    else
        # No cost change for "maintain", "optimize", etc. in this simple model
        echo "0.00"
        return
    fi

    # Check if we have a target and pricing data for both
    if [[ -z "$target_instance_type" ]]; then
        log "DEBUG" "No target instance type found for ${current_instance_type} with recommendation ${recommendation}."
        echo "0.00"
        return
    fi

    local current_cost_per_hour=${INSTANCE_COSTS[$current_instance_type]}
    local target_cost_per_hour=${INSTANCE_COSTS[$target_instance_type]}

    if [[ -z "$current_cost_per_hour" || -z "$target_cost_per_hour" ]]; then
        log "WARN" "Missing pricing data for cost calculation. Current: '${current_instance_type}' (${current_cost_per_hour}), Target: '${target_instance_type}' (${target_cost_per_hour})."
        echo "0.00"
        return
    fi

    # Calculate the monthly savings (or cost increase)
    # Savings are positive, cost increases are negative
    local monthly_savings
    monthly_savings=$(echo "scale=2; (${current_cost_per_hour} - ${target_cost_per_hour}) * 24 * 30" | bc)

    echo "$monthly_savings"
}

# Function to draw the progress bar
draw_progress_bar() {
    local current=$1
    local total=$2
    local hostname=$3
    local percent=$((current * 100 / total))
    local filled_len=$((percent / 2))
    local empty_len=$((50 - filled_len))

    local filled_bar
    filled_bar=$(printf "%${filled_len}s" | tr ' ' '█')
    local empty_bar
    empty_bar=$(printf "%${empty_len}s" | tr ' ' '-')

    printf "\rAnalyzing Hosts: [${GREEN}%s%s${NC}] %d%% | Now: %s" "$filled_bar" "$empty_bar" "$percent" "$hostname"
}

# Function to mount NFS
mount_nfs() {
    if mountpoint -q "$NFS_MOUNT"; then
        log "INFO" "NFS is already mounted at ${NFS_MOUNT}. Skipping mount."
    else
        log "INFO" "Attempting to mount NFS share: ${NFS_SERVER} to ${NFS_MOUNT}"
        if mount -t nfs "${NFS_SERVER}" "${NFS_MOUNT}"; then
            log "INFO" "NFS mounted successfully."
            NFS_MOUNTED_BY_SCRIPT=true
        else
            log "ERROR" "Failed to mount NFS share: ${NFS_SERVER} to ${NFS_MOUNT}. Check permissions or network. Exiting."
            exit 1
        fi
    fi
}

# Safe numeric comparison function
safe_compare() {
    # Regex handles negative numbers for validation purposes
    local n1=$(echo "$1" | awk '{print ($1 ~ /^-?[0-9]+(\.[0-9]+)?$/ ? $1 : 0)}')
    local op="$2"
    local n2=$(echo "$3" | awk '{print ($1 ~ /^-?[0-9]+(\.[0-9]+)?$/ ? $1 : 0)}')
    # Using awk for floating point comparisons
    awk -v n1="$n1" -v op="$op" -v n2="$n2" 'BEGIN {
        if (op == "<") exit !(n1 < n2);
        if (op == "<=") exit !(n1 <= n2);
        if (op == ">") exit !(n1 > n2);
        if (op == ">=") exit !(n1 >= n2);
        if (op == "==") exit !(n1 == n2);
    }'
    return $?
}

# Function to parse SAR CPU data
parse_cpu_data() {
    local sar_data_dir="$1/sar_data"
    local total_avg_sum=0
    local max_overall_peak=0
    local days_processed=0
    local daily_averages=""

    log "DEBUG" "Parsing CPU data from: ${sar_data_dir}"
    if [[ ! -d "$sar_data_dir" ]]; then
        log "WARN" "SAR data directory not found: ${sar_data_dir}. Returning 0|0|."
        echo "0|0|"
        return
    fi

    for day in $(seq 1 "$args_days"); do
        local sar_file="${sar_data_dir}/cpu_day${day}.txt"

        if [[ ! -s "$sar_file" ]]; then
            daily_averages+=" 0" # Add a 0 for missing days to keep trend data consistent
            if [[ -f "$sar_file" ]]; then
                log "WARN" "SAR CPU file exists but is empty, skipping for this day: ${sar_file}"
            else
                log "DEBUG" "SAR CPU file not found: ${sar_file}"
            fi
            continue
        fi

        local header
        header=$(grep -m 1 "%user" "$sar_file")
        if [[ -z "$header" ]]; then
            daily_averages+=" 0"
            log "WARN" "Could not find a valid CPU header in ${sar_file}. Skipping."
            continue
        fi

        local user_col system_col idle_col
        user_col=$(echo "$header" | awk '{for(i=1;i<=NF;i++) if($i=="%user") print i}')
        system_col=$(echo "$header" | awk '{for(i=1;i<=NF;i++) if($i=="%system") print i}')
        idle_col=$(echo "$header" | awk '{for(i=1;i<=NF;i++) if($i=="%idle") print i}')

        if [[ -z "$user_col" || -z "$system_col" || -z "$idle_col" ]]; then
            daily_averages+=" 0"
            log "WARN" "Could not find all required CPU columns in ${sar_file}. Skipping."
            continue
        fi

        local daily_stats
        daily_stats=$(awk -v user_col="$user_col" -v system_col="$system_col" -v idle_col="$idle_col" '
            /^[0-9]{2}:[0-9]{2}:[0-9]{2}/ {
                if (NF >= user_col && NF >= system_col && NF >= idle_col) {
                    sum_idle += $idle_col;
                    current_workload = $user_col + $system_col;
                    if (current_workload > peak_workload) {
                        peak_workload = current_workload;
                    }
                    count++;
                }
            }
            END {
                if (count > 0) {
                    avg_idle = sum_idle / count;
                    avg_busy = 100 - avg_idle;
                    printf "%.2f|%.2f", avg_busy, peak_workload;
                } else {
                    print "0|0";
                }
            }' "$sar_file")

        local day_avg=$(echo "$daily_stats" | awk -F'|' '{print $1}')
        local day_peak=$(echo "$daily_stats" | awk -F'|' '{print $2}')

        if safe_compare "$day_avg" ">=" "0" && safe_compare "$day_peak" ">=" "0"; then
            total_avg_sum=$(echo "scale=2; $total_avg_sum + $day_avg" | bc)
            daily_averages+=" $day_avg"
            if safe_compare "$day_peak" ">" "$max_overall_peak"; then
                max_overall_peak="$day_peak"
            fi
            days_processed=$((days_processed + 1))
        else
            daily_averages+=" 0"
            log "WARN" "Invalid CPU avg/peak data calculated for ${sar_file}. Avg: '${day_avg}', Peak: '${day_peak}'. Skipping."
        fi
    done

    if [[ "$days_processed" -gt 0 ]]; then
        local final_avg=$(echo "scale=2; $total_avg_sum / $days_processed" | bc)
        if ! safe_compare "$final_avg" ">=" 0 || ! safe_compare "$final_avg" "<=" 100; then
            log "ERROR" "Corrupted CPU data detected. Final average (${final_avg}) is outside valid 0-100 range."
            echo "Error|Error|"
        else
            echo "${final_avg}|${max_overall_peak}|${daily_averages:1}"
        fi
    else
        log "WARN" "No valid CPU data processed for ${sar_data_dir} over ${args_days} days. Returning 0|0|."
        echo "0|0|"
    fi
}

# Function to parse SAR Memory data based on AVAILABLE memory
parse_memory_data() {
    local sar_data_dir="$1"
    local os_type="$2" # "modern" or "legacy"
    local total_avg_sum=0
    local max_overall_peak=0
    local days_processed=0
    local daily_averages=""

    log "DEBUG" "Parsing Memory data from: ${sar_data_dir} with OS type: ${os_type}"
    if [[ ! -d "$sar_data_dir/sar_data" ]]; then
        log "WARN" "SAR data directory not found: ${sar_data_dir}/sar_data. Returning 0|0|."
        echo "0|0|"
        return
    fi

    for day in $(seq 1 "$args_days"); do
        local sar_file="${sar_data_dir}/sar_data/mem_day${day}.txt"

        if [[ ! -s "$sar_file" ]]; then
            daily_averages+=" 0"
            if [[ -f "$sar_file" ]]; then
                log "WARN" "SAR Memory file exists but is empty, skipping for this day: ${sar_file}"
            else
                log "DEBUG" "SAR Memory file not found: ${sar_file}"
            fi
            continue
        fi

        local daily_stats
        daily_stats=$(awk -v os_type="$os_type" '
            BEGIN {
                # Determine logic path based on os_type passed from shell
                is_modern = (os_type == "modern");
                headers_locked = 0;
                # Initialize column vars to 0 to be safe
                percent_used_col=0; free_col=0; used_col=0; avail_col=0;
            }

            # This block runs on every line until the required headers are found and locked in.
            !headers_locked {
                if (is_modern) {
                    # For modern systems, we ONLY need the "%memused" column.
                    if ($0 ~ /%memused/) {
                        for(i=1; i<=NF; i++) {
                            if ($i == "%memused") percent_used_col=i;
                        }
                        if (percent_used_col > 0) headers_locked=1;
                    }
                } else {
                    # For legacy systems, we need the columns for the manual calculation.
                    if ($0 ~ /kbmemfree/) {
                        for(i=1; i<=NF; i++) {
                            if($i=="kbmemfree") free_col=i;
                            if($i=="kbmemused") used_col=i;
                            if($i=="kbavail") avail_col=i;
                        }
                        if (free_col > 0 && used_col > 0 && avail_col > 0) headers_locked=1;
                    }
                }
                # If we just locked the headers, skip to the next line to avoid processing the header row as data.
                if (headers_locked) next;
            }

            # Process data lines only after headers are locked.
            # We also check for a timestamp format to avoid summary lines like "Average:".
            headers_locked && /^[0-9]{2}:[0-9]{2}:[0-9]{2}/ {
                if (is_modern) {
                    percent_used = $percent_used_col;
                } else {
                    # Legacy calculation
                    total_mem = $free_col + $used_col;
                    true_used = total_mem - $avail_col;
                    if (total_mem > 0) {
                       percent_used = (true_used / total_mem) * 100;
                    } else {
                       percent_used = 0;
                    }
                }

                sum += percent_used;
                if (percent_used > peak) {
                    peak = percent_used;
                }
                count++;
            }

            END {
                if (!headers_locked) {
                    print "Error:MissingCols|0";
                } else if (count > 0) {
                    printf "%.2f|%.2f", sum/count, peak;
                } else {
                    print "0|0";
                }
            }' "$sar_file")

        local day_avg=$(echo "$daily_stats" | awk -F'|' '{print $1}')

        # Check for the custom error message from awk
        if [[ "$day_avg" == "Error:MissingCols" ]]; then
            log "WARN" "Could not find required memory columns in ${sar_file} for os_type '${os_type}'. Skipping day."
            daily_averages+=" 0"
            continue
        fi

        local day_peak=$(echo "$daily_stats" | awk -F'|' '{print $2}')

        # Final sanity check on values before proceeding
        if safe_compare "$day_avg" ">=" "0" && safe_compare "$day_peak" ">=" "0"; then
            total_avg_sum=$(echo "scale=2; $total_avg_sum + $day_avg" | bc)
            daily_averages+=" $day_avg"
            if safe_compare "$day_peak" ">" "$max_overall_peak"; then
                max_overall_peak="$day_peak"
            fi
            days_processed=$((days_processed + 1))
        else
            daily_averages+=" 0"
            log "WARN" "Invalid memory avg/peak data calculated for ${sar_file}. Avg: '${day_avg}', Peak: '${day_peak}'. Skipping."
        fi
    done

    if [[ "$days_processed" -gt 0 ]]; then
        local final_avg=$(echo "scale=2; $total_avg_sum / $days_processed" | bc)
        echo "${final_avg}|${max_overall_peak}|${daily_averages:1}"
    else
        log "WARN" "No valid Memory data processed for ${sar_data_dir} over ${args_days} days. Returning 0|0|."
        echo "0|0|"
    fi
}

# Function to parse cloud metadata
parse_cloud_info() {
    local backup_dir="$1"
    local cloud_info_path_xml="${backup_dir}/cloud_info/cloud_metadata.xml"
    local cloud_info_path_html="${backup_dir}/cloud_info/cloud_report.html"

    log "DEBUG" "Parsing cloud metadata from: ${backup_dir}/cloud_info"

    local platform="Unknown"
    local instance_type="Unknown"
    local vcpu="?"
    local memory_gb="?"

    if [[ -f "$cloud_info_path_xml" ]]; then
        log "DEBUG" "Attempting to parse cloud_metadata.xml: ${cloud_info_path_xml}"
        if command -v xmllint &>/dev/null; then
            platform=$(xmllint --xpath "string(//platform)" "$cloud_info_path_xml" 2>/dev/null | tr -d '\n' | sed 's/Microsoft Azure/Azure/g' || echo "Unknown")
            if [[ "$platform" == *"OCI"* ]]; then
                instance_type=$(xmllint --xpath "string(//oci_instance/shape)" "$cloud_info_path_xml" 2>/dev/null | tr -d '\n' || echo "Unknown")
            elif [[ "$platform" == *"Azure"* ]]; then
                instance_type=$(xmllint --xpath "string(//vm_size)" "$cloud_info_path_xml" 2>/dev/null | tr -d '\n' || echo "Unknown")
            fi
        else
            log "WARN" "xmllint not found. Falling back to grep for cloud_metadata.xml."
            platform=$(grep -oP '(?<=<platform>)[^<]+' "$cloud_info_path_xml" | head -1 | sed 's/Microsoft Azure/Azure/g' || echo "Unknown")
            if [[ "$platform" == *"OCI"* ]]; then
                instance_type=$(grep -oP '(?<=<shape>)[^<]+' "$cloud_info_path_xml" | head -1 || echo "Unknown")
            elif [[ "$platform" == *"Azure"* ]]; then
                instance_type=$(grep -oP '(?<=<vm_size>)[^<]+' "$cloud_info_path_xml" | head -1 || echo "Unknown")
            fi
        fi
        log "DEBUG" "From XML: Platform='${platform}', InstanceType='${instance_type}'"
    fi

    if [[ -f "$cloud_info_path_html" ]]; then
        log "DEBUG" "Attempting to parse cloud_report.html for detailed instance info: ${cloud_info_path_html}"

        local html_vcpu=$(grep -oP '<tr><td>CPU Cores</td><td>\K\d+' "$cloud_info_path_html" | head -1 || echo "?")
        if [[ "$html_vcpu" != "?" ]]; then
            vcpu="$html_vcpu"
        fi

        local html_mem_raw=$(grep -oP '<tr><td>Total Memory</td><td>\K\d+(Gi|Mi)?' "$cloud_info_path_html" | head -1 || echo "?")
        if [[ "$html_mem_raw" != "?" ]]; then
            local mem_value=$(echo "$html_mem_raw" | sed 's/[^0-9.]*//g')
            local mem_unit=$(echo "$html_mem_raw" | grep -oP '(Gi|Mi)' || echo "Gi")
            if [[ "$mem_unit" == "Gi" ]]; then
                memory_gb="$mem_value"
            elif [[ "$mem_unit" == "Mi" ]]; then
                memory_gb=$(echo "scale=2; $mem_value / 1024" | bc || echo "?")
            else
                memory_gb="$mem_value"
            fi
        fi

        if [[ "$platform" == "Unknown" ]]; then
            platform=$(grep -oP '<h2>\K[^<]+' "$cloud_info_path_html" | grep -i 'cloud' | head -1 | sed -E 's/.*(Azure|AWS|OCI).*/\1/' || echo "Unknown")
            log "DEBUG" "Updated Platform from HTML: ${platform}"
        fi

        if [[ "$instance_type" == "Unknown" ]]; then
            local html_shape=$(grep -oP '<tr><td>(Shape|VM Size)</td><td>\K[a-zA-Z0-9\._-]+' "$cloud_info_path_html" | head -1 || echo "Unknown")
            if [[ "$html_shape" != "Unknown" ]]; then
                instance_type="$html_shape"
                log "DEBUG" "Updated Instance Type from HTML (Shape/VM Size): ${instance_type}"
            fi
        fi
    fi

    echo "${platform}|${instance_type}|${vcpu}|${memory_gb}"
}

# Function to parse storage data from df -h output
parse_storage_data() {
    local backup_dir="$1"
    local df_file="${backup_dir}/df_h.txt"

    if [[ ! -s "$df_file" ]]; then
        return
    fi

    # Use a portable awk script to parse the df -h output and apply all filtering rules
    awk '
    # Main processing block - runs for every line
    NR > 1 {
        # Rule 1: Exclude network & virtual filesystems
        if ($1 ~ /:/ || $1 ~ /^\/\// || $1 ~ /^(devtmpfs|tmpfs)$/) {
            next
        }
        # Rule 2: Exclude specific OS and application mount points
        mount_point = $NF;
        if (mount_point == "/") { next }
        if (substr(mount_point, 1, 5) == "/boot") { next }
        if (substr(mount_point, 1, 4) == "/run") { next }
        if (substr(mount_point, 1, 4) == "/var") { next }
        if (substr(mount_point, 1, 4) == "/opt") { next }
        if (substr(mount_point, 1, 4) == "/usr") { next }
        if (substr(mount_point, 1, 5) == "/pool") { next }

        # Rule 3: Pass raw data to shell for robust parsing and filtering
        printf "%s|%s|%s|%s\n", $NF, $2, $3, $5;
    }' "$df_file"
}

# Function to classify host based on utilization
classify_zone() {
    local avg_cpu="$1"
    local avg_mem="$2"
    local vcpu_count="$3"
    local mem_gb="$4"
    local peak_cpu="$5"
    local peak_mem="$6"

    # 1. Determine independent resource states
    local cpu_state="normal"
    if safe_compare "$avg_cpu" ">" "$HOT_CPU_MIN"; then cpu_state="hot";
    elif safe_compare "$avg_cpu" "<" "$COLD_CPU_MAX"; then cpu_state="cold"; fi

    local mem_state="normal"
    if safe_compare "$avg_mem" ">" "$HOT_MEM_MIN"; then mem_state="hot";
    elif safe_compare "$avg_mem" "<" "$COLD_MEM_MAX"; then mem_state="cold"; fi

    # 2. Determine the 'natural' zone and recommendation
    local zone="optimal"
    local recommendation="maintain"

    if [[ "$cpu_state" == "hot" || "$mem_state" == "hot" ]]; then
        zone="hot"
        recommendation="upsize"
    elif [[ "$cpu_state" == "cold" && "$mem_state" == "cold" ]]; then
        zone="cold"
        recommendation="downsize"
    elif [[ "$cpu_state" == "normal" && "$mem_state" == "cold" ]]; then
        zone="optimize"
        recommendation="Optimize (Memory)"
    elif [[ "$cpu_state" == "cold" && "$mem_state" == "normal" ]]; then
        zone="optimize"
        recommendation="Optimize (CPU)"
    fi

    # 3. Apply exemptions ONLY if the system is NOT Hot
    if [[ "$zone" != "hot" ]]; then
        # High Peak Exemption
        if safe_compare "$peak_cpu" ">" "$HIGH_PEAK_THRESHOLD" || safe_compare "$peak_mem" ">" "$HIGH_PEAK_THRESHOLD"; then
            log "DEBUG" "Applying High Peak exemption. Peak CPU: ${peak_cpu}, Peak Mem: ${peak_mem}. Overriding zone from ${zone} to optimal."
            zone="optimal"
            recommendation="Maintain (High Peak)"
        # Small CPU Exemption Logic
        elif safe_compare "$vcpu_count" "<=" "2"; then
            if safe_compare "$mem_gb" "<=" "8"; then
                log "DEBUG" "Applying Small System exemption (CPU<=2, Mem<=8). Overriding zone from ${zone} to optimal."
                zone="optimal"
                recommendation="Maintain (Exempt)"
            # If CPU is small but Mem is large and underutilized, recommend memory optimization only
            elif [[ "$mem_state" == "cold" ]]; then
                log "DEBUG" "Applying Small CPU / Large Mem exemption. Recommending memory optimization."
                zone="optimize"
                recommendation="Optimize (Memory)"
            # If CPU is small and underutilized but Mem is normal, exempt the CPU optimization
            else
                log "DEBUG" "Applying Small CPU exemption (CPU<=2). Overriding zone from ${zone} to optimal."
                zone="optimal"
                recommendation="Maintain (Exempt)"
            fi
        fi
    fi

    echo "${zone}|${recommendation}|${cpu_state}|${mem_state}"
}

# Function to calculate Standard Deviation and Coefficient of Variation
calculate_stats() {
    local data_points="$1"

    # Return 0 if no data points
    if [[ -z "$data_points" ]]; then
        echo "0"
        return
    fi

    # Use awk for all calculations
    awk -v data="$data_points" '
    BEGIN {
        split(data, arr, " ");
        n = 0;
        for (i in arr) {
            if (arr[i] != "") {
                # Only use non-negative values for volatility calculation
                if (arr[i] >= 0) {
                    n++;
                    sum += arr[i];
                    sum_sq += arr[i]^2;
                }
            }
        }

        if (n == 0) {
            print 0;
            exit;
        }

        mean = sum / n;
        if (n > 1) {
            stdev = sqrt((sum_sq - (sum^2 / n)) / (n - 1));
        } else {
            stdev = 0;
        }

        if (mean > 0) {
            cv = (stdev / mean) * 100;
        } else {
            cv = 0;
        }

        printf "%.0f", cv;
    }'
}

# Function to generate an SVG sparkline chart
generate_svg_sparkline() {
    local data_points="$1"
    local color="$2"

    if [[ -z "$data_points" ]]; then
        echo ""
        return
    fi

    local -a values=($data_points)
    local max_val=100 # Use 100 as a fixed max for percentage data

    local point_str=""
    local x_coord=0
    local x_step=20 # 120 width / (7-1) points = 20

    for val in "${values[@]}"; do
        # Clamp values between 0 and 100 for charting
        local clean_val=$(echo "$val" | awk '{if ($1<0) print 0; else if ($1>100) print 100; else print $1}')
        local y_coord=$(echo "scale=2; 40 - ($clean_val / $max_val) * 40" | bc)
        point_str+="${x_coord},${y_coord} "
        x_coord=$((x_coord + x_step))
    done

    # Generate the SVG code
    printf '<svg width="120" height="40" viewBox="0 0 120 40" xmlns="http://www.w3.org/2000/svg"><polyline points="%s" style="fill:none;stroke:%s;stroke-width:2"/></svg>' \
        "${point_str% }" "$color"
}


# Function to analyze a single host
analyze_host() {
    local hostname="$1"
    local backup_dir="$2"

    log "DEBUG" "Analyzing host: ${hostname} from backup: ${backup_dir}"

    if [[ ! -d "$backup_dir" ]]; then
        log "ERROR" "Backup directory not found for ${hostname}: ${backup_dir}. Skipping analysis."
        printf "%s|Backup directory not found\n" "$hostname" >> "$SKIPPED_FILE"
        return 1
    fi

    local sysstat_file="${backup_dir}/sar_data/sysstat_status.txt"
    if [[ -f "$sysstat_file" ]]; then
        if grep -q "could not be found" "$sysstat_file"; then
            log "WARN" "Host ${hostname} is missing sysstat package. Skipping analysis."
            printf "%s|Sysstat service not found on host\n" "$hostname" >> "$SKIPPED_FILE"
            return 1
        fi
    fi

    # --- Performance Analysis ---
    # Determine OS type to select correct memory calculation logic
    local os_type="legacy"
    local release_file="${backup_dir}/instance_metadata.txt"
    if [[ -f "$release_file" ]] && grep -q -E "^os_version:.*release 9" "$release_file"; then
        os_type="modern"
    fi
    log "DEBUG" "OS detection for ${hostname}: type='${os_type}'"


    local cpu_data=$(parse_cpu_data "$backup_dir")
    local mem_data=$(parse_memory_data "$backup_dir" "$os_type")
    local cloud_info=$(parse_cloud_info "$backup_dir")

    # --- Storage Analysis ---
    log "DEBUG" "Parsing storage data for ${hostname}"
    parse_storage_data "$backup_dir" > "${TEMP_DIR}/${hostname}_storage.txt"


    local avg_cpu=$(echo "$cpu_data" | awk -F'|' '{print $1}')
    local peak_cpu=$(echo "$cpu_data" | awk -F'|' '{print $2}')
    local daily_cpu_avgs=$(echo "$cpu_data" | awk -F'|' '{print $3}')

    local avg_mem=$(echo "$mem_data" | awk -F'|' '{print $1}')
    local peak_mem=$(echo "$mem_data" | awk -F'|' '{print $2}')
    local daily_mem_avgs=$(echo "$mem_data" | awk -F'|' '{print $3}')

    local platform=$(echo "$cloud_info" | awk -F'|' '{print $1}')
    local instance_type=$(echo "$cloud_info" | awk -F'|' '{print $2}')
    local vcpu_count=$(echo "$cloud_info" | awk -F'|' '{print $3}')
    local memory_total_gb=$(echo "$cloud_info" | awk -F'|' '{print $4}')

    local original_vcpu_count="$vcpu_count"

    # --- TEMPORARY OCI OCPU FIX ---
    # TODO: This is a temporary workaround to correct the OCI vCPU vs OCPU reporting issue.
    # The vCPU count is halved for OCI instances because the source data from cloud_report.html
    # provides the OS-level vCPU count, which is double the actual OCPU count.
    # The permanent fix should be implemented in the cloud_identify.sh script to query
    # the metadata for the correct 'ocpus' value directly.
    if [[ "$platform" == *"OCI"* && "$vcpu_count" =~ ^[0-9]+$ && "$vcpu_count" -gt 0 ]]; then
        vcpu_count=$((vcpu_count / 2))
        log "DEBUG" "Applied temporary OCI OCPU fix. Original vCPU count: ${original_vcpu_count}, corrected OCPU count: ${vcpu_count}"
    fi
    # --- END TEMPORARY FIX ---

    log "DEBUG" "Host: ${hostname} | CPU Avg/Peak: ${avg_cpu}%/${peak_cpu}% | Mem Avg/Peak: ${avg_mem}%/${peak_mem}% | Cloud: ${platform}/${instance_type} (${vcpu_count} Cores, ${memory_total_gb}GB)"

    # --- Data Error Handling ---
    if [[ "$avg_cpu" == "Error" || "$avg_mem" == "Error" ]]; then
        log "ERROR" "Host ${hostname} has a data error. Assigning to Optimal for manual review."
        printf "%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%.2f\n" \
            "$hostname" "${avg_cpu//Error/N/A}" "N/A" "${avg_mem//Error/N/A}" "N/A" \
            "optimal" "Review (Data Error)" \
            "$vcpu_count" "$memory_total_gb" "$platform" "$instance_type" \
            "error" "error" "0" "0" "" "" "0.00" >> "$RESULTS_FILE"
        return 0
    fi

    if safe_compare "$avg_cpu" "==" "0" && safe_compare "$avg_mem" "==" "0" && safe_compare "$peak_cpu" == "0" && safe_compare "$peak_mem" == "0"; then
        log "WARN" "Host has no utilization data. Marking for observation."
        printf "%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%.2f\n" \
            "$hostname" "0" "0" "0" "0" "unknown" "monitor" \
            "$vcpu_count" "$memory_total_gb" "$platform" "$instance_type" \
            "n/a" "n/a" "0" "0" "" "" "0.00" >> "$RESULTS_FILE"
        return 0
    fi

    local zone_data=$(classify_zone "$avg_cpu" "$avg_mem" "$vcpu_count" "$memory_total_gb" "$peak_cpu" "$peak_mem")
    local zone=$(echo "$zone_data" | awk -F'|' '{print $1}')
    local recommendation=$(echo "$zone_data" | awk -F'|' '{print $2}')
    local cpu_state=$(echo "$zone_data" | awk -F'|' '{print $3}')
    local mem_state=$(echo "$zone_data" | awk -F'|' '{print $4}')

    # Calculate potential cost savings
    local monthly_savings
    monthly_savings=$(calculate_cost_implication "$instance_type" "$recommendation")


    # Calculate advanced stats
    local cpu_cv=$(calculate_stats "$daily_cpu_avgs")
    local mem_cv=$(calculate_stats "$daily_mem_avgs")
    local cpu_sparkline=$(generate_svg_sparkline "$daily_cpu_avgs" "#3B82F6")
    local mem_sparkline=$(generate_svg_sparkline "$daily_mem_avgs" "#10B981")

    printf "%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s\n" \
        "$hostname" "$avg_cpu" "$peak_cpu" "$avg_mem" "$peak_mem" \
        "$zone" "$recommendation" \
        "$vcpu_count" "$memory_total_gb" "$platform" "$instance_type" \
        "$cpu_state" "$mem_state" \
        "$cpu_cv" "$mem_cv" "$cpu_sparkline" "$mem_sparkline" "$monthly_savings" >> "$RESULTS_FILE"

    # --- Append to Historical Trend File ---
    if [[ "$zone" != "unknown" ]]; then
        local sanitized_platform
        sanitized_platform=$(echo "$platform" | tr ',' ';')
        local sanitized_instance_type
        sanitized_instance_type=$(echo "$instance_type" | tr ',' ';')

        printf "%s,%s,%.2f,%.2f,%.2f,%.2f,%s,%s,%s,%s,%s,%s\n" \
            "$(date +%F)" \
            "$hostname" \
            "$avg_cpu" \
            "$peak_cpu" \
            "$avg_mem" \
            "$peak_mem" \
            "$zone" \
            "$recommendation" \
            "$sanitized_instance_type" \
            "$sanitized_platform" \
            "$vcpu_count" \
            "$memory_total_gb" >> "$TRENDS_FILE"
    fi

    log "INFO" "Analysis complete for ${hostname}. Zone: ${zone}, Recommendation: ${recommendation}"
}

# Bash function to convert human-readable sizes to GB
# Takes one argument, e.g., "100G", "2.5T", "900M"
# Returns size in GB, allows for floating point
convert_to_gb() {
    local size_str="$1"
    local val unit

    # Return 0 if input is empty
    if [[ -z "$size_str" ]]; then
        echo "0"
        return
    fi

    # Extract the numeric part and the unit
    val=$(echo "$size_str" | sed -e 's/[KkMmGgTtPp]//g')
    unit=$(echo "$size_str" | sed -e 's/[0-9\.]//g' | tr '[:lower:]' '[:upper:]' | head -c1)

    local scale=4 # Set precision for bc
    case "$unit" in
        "K") echo "scale=${scale}; ${val} / 1024 / 1024" | bc ;;
        "M") echo "scale=${scale}; ${val} / 1024" | bc ;;
        "G") echo "$val" ;;
        "T") echo "scale=${scale}; ${val} * 1024" | bc ;;
        "P") echo "scale=${scale}; ${val} * 1024 * 1024" | bc ;;
        *)   echo "0" ;;
    esac
}

# Function to analyze historical trends for sustained status
analyze_historical_trends() {
    log "INFO" "Analyzing historical trends for sustained status and Monthly KPI..."

    if [[ ! -f "$TRENDS_FILE" ]]; then
        log "WARN" "Trends file not found, skipping historical analysis."
        # Create empty files to prevent errors downstream
        > "$HISTORICAL_SUMMARY_FILE"
        > "$SUSTAINED_STATUS_FILE"
        return
    fi

    # Get today's date and the date 30 days ago for comparison
    local thirty_days_ago=$(date -d "30 days ago" +%F)

    # This awk script does three things:
    # 1. Gathers the zone for each host from ~30 days ago.
    # 2. Calculates the fleet efficiency KPI from ~30 days ago.
    # 3. Compares the ~30 day status with the current status to find sustained issues.
    awk -F'|' '
        # Pass shell variables to awk
        BEGIN {
            FS_HIST = ",";
            FS_CUR = "|";

            # Shell variables
            trends_file = ARGV[2];
            thirty_days_ago = "'"$thirty_days_ago"'";

            # --- Block 1: Read the historical trends file into memory ---
            while ((getline line < trends_file) > 0) {
                # line[1]=Date, [2]=Host, [7]=Zone
                split(line, fields, FS_HIST);
                hist_host = fields[2];
                hist_date = fields[1];

                # Store the zone from ~30 days ago (or the closest prior date)
                if (hist_date <= thirty_days_ago) {
                    if (!(hist_host in thirty_day_date) || hist_date > thirty_day_date[hist_host]) {
                        thirty_day_date[hist_host] = hist_date;
                        thirty_day_zone[hist_host] = fields[7];
                    }
                }
            }
            close(trends_file);
            # --- Block 2: Calculate the 30-day fleet efficiency KPI ---
            thirty_day_total = 0;
            thirty_day_optimal = 0;
            for (host in thirty_day_zone) {
                thirty_day_total++;
                if (thirty_day_zone[host] == "optimal") {
                    thirty_day_optimal++;
                }
            }
            thirty_day_efficiency = (thirty_day_total > 0) ? (thirty_day_optimal / thirty_day_total) * 100 : 0;

            # Output for other functions to use
            printf "MONTHLY_EFFICIENCY|%.1f\n", thirty_day_efficiency > "'"$HISTORICAL_SUMMARY_FILE"'";
        }

        # --- Block 3: Process the current results file and compare to 30-day history ---
        # $1=Host, $2=AvgCPU, $4=AvgMem, $6=Zone, $8=Cores, $9=MemGB, $11=InstanceType, $18=Savings
        {
            FS = FS_CUR;
            current_host = $1;
            current_zone = $6;

            # Check if this host existed 30 days ago and if its status is sustained
            if (current_host in thirty_day_zone) {
                prev_zone = thirty_day_zone[current_host];
                # Check for sustained Hot status
                if (current_zone == "hot" && prev_zone == "hot") {
                    printf "SUSTAINED_HOT|%s|%s|%s|%s|%.1f|%.1f|%s\n", current_host, $11, $8, $9, $2, $4, $18;
                }
                # Check for sustained Cold status
                else if (current_zone == "cold" && prev_zone == "cold") {
                    printf "SUSTAINED_COLD|%s|%s|%s|%s|%.1f|%.1f|%s\n", current_host, $11, $8, $9, $2, $4, $18;
                }
            }
        }
    ' "$RESULTS_FILE" "$TRENDS_FILE" > "$SUSTAINED_STATUS_FILE"

    log "INFO" "Historical trend analysis complete."
}


# Function to generate HTML report
generate_html_report() {
    local data_file="$RESULTS_FILE"
    local stale_file="$STALE_FILE"
    local sustained_status_file="$SUSTAINED_STATUS_FILE"
    local report_file="${REPORTS_DIR}/capacity_report_$(date +%Y%m%d_%H%M%S).html"

    local total_hosts=0
    local cold_count=0
    local hot_count=0
    local optimize_count=0
    local optimal_count=0
    local action_count=0
    local no_data_count=0
    local stale_count=0
    local analyzed_rows_count=0
    local storage_table_rows=""
    local storage_rows_count=0
    local underutilized_storage_count=0
    local efficiency_score="0.0"
    local total_potential_savings=0.00

    local analyzed_rows=""
    local no_data_rows=""
    local stale_rows=""

    # --- Build Sustained Status Tables ---
    local sustained_hot_html=""
    local sustained_cold_html=""
    local sustained_hot_count=0
    local sustained_cold_count=0

    if [[ -s "$sustained_status_file" ]]; then
        while IFS='|' read -r category host instance cores mem_gb cpu_pct mem_pct monthly_savings; do
            local cpu_unit="Cores"
             if [[ "$instance" == *"VM.Standard.E"* || "$instance" == *"VM.Standard.D"* ]]; then # Simple check for OCI
                cpu_unit="OCPUs"
            fi
            local savings_cell=""
            if safe_compare "${monthly_savings:-0}" ">" "0"; then
                savings_cell="<td class=\"savings-positive\">\$$(printf "%.2f" "$monthly_savings")</td>"
            elif safe_compare "${monthly_savings:-0}" "<" "0"; then
                local cost_increase
                cost_increase=$(echo "$monthly_savings * -1" | bc)
                savings_cell="<td class=\"savings-negative\">-\$$(printf "%.2f" "$cost_increase")</td>"
            else
                savings_cell="<td>-</td>"
            fi

            local row="<tr><td><strong>${host}</strong></td><td>${instance}</td><td>${cores} ${cpu_unit}</td><td>${mem_gb} GB</td><td>${cpu_pct}%</td><td>${mem_pct}%</td>${savings_cell}</tr>"
            case "$category" in
                "SUSTAINED_HOT")
                    sustained_hot_html+="$row"
                    ((sustained_hot_count++))
                    ;;
                "SUSTAINED_COLD")
                    sustained_cold_html+="$row"
                    ((sustained_cold_count++))
                    ;;
            esac
        done < "$sustained_status_file"
    fi

    # --- Build Main Compute and Skipped/Stale Tables ---
    if [[ -s "$data_file" ]]; then
        total_hosts=$(wc -l < "$data_file" | tr -d ' ')
        # Use precise awk field matching instead of grep to prevent double-counting
        hot_count=$(awk -F'|' '$6 == "hot"' "$data_file" | wc -l)
        optimize_count=$(awk -F'|' '$6 == "optimize"' "$data_file" | wc -l)
        cold_count=$(awk -F'|' '$6 == "cold"' "$data_file" | wc -l)
        optimal_count=$(awk -F'|' '$6 == "optimal"' "$data_file" | wc -l)

        cold_count=${cold_count:-0}
        hot_count=${hot_count:-0}
        optimize_count=${optimize_count:-0}
        optimal_count=${optimal_count:-0}
        action_count=$((cold_count + hot_count + optimize_count))

        local sorted_data
        sorted_data=$(awk 'BEGIN{FS=OFS="|"} { if ($6=="hot") print "1",$0; else if ($6=="optimize") print "2",$0; else if ($6=="cold") print "3",$0; else if ($6=="unknown") print "5",$0; else print "4",$0 }' "$data_file" | sort -t'|' -k1,1n | cut -d'|' -f2-)

        while IFS='|' read -r hostname cpu_avg cpu_peak mem_avg mem_peak zone recommendation cpu_count memory_total cloud_provider instance_type cpu_state mem_state cpu_cv mem_cv cpu_sparkline mem_sparkline monthly_savings; do
            [[ -z "$hostname" ]] && continue
            cpu_avg=${cpu_avg:-0.0}; cpu_peak=${cpu_peak:-0.0}; mem_avg=${mem_avg:-0.0}; mem_peak=${mem_peak:-0.0}
            cpu_count=${cpu_count:-?}; memory_total=${memory_total:-?}; cloud_provider=${cloud_provider:-Unknown}; instance_type=${instance_type:-Unknown};
            zone=${zone:-unknown}; monthly_savings=${monthly_savings:-0.00}

            # Accumulate total potential savings
            if safe_compare "$monthly_savings" ">" "0"; then
                total_potential_savings=$(echo "scale=2; $total_potential_savings + $monthly_savings" | bc)
            fi

            if [[ "$zone" == "unknown" ]]; then
                no_data_count=$((no_data_count + 1))
                no_data_rows+="
                <tr>
                    <td><strong>$hostname</strong></td>
                    <td>${instance_type}</td>
                    <td>${cloud_provider}</td>
                    <td>${cpu_count} vCPUs</td>
                    <td>${memory_total}GB RAM</td>
                </tr>"
            else
                analyzed_rows_count=$((analyzed_rows_count + 1))
                local cpu_color_class="util-normal"
                local mem_color_class="util-normal"

                if [[ "$zone" == "optimize" ]]; then
                    [[ "$cpu_state" == "cold" ]] && cpu_color_class="util-optimize"
                    [[ "$mem_state" == "cold" ]] && mem_color_class="util-optimize"
                elif [[ "$zone" == "cold" ]]; then
                    cpu_color_class="util-cold"; mem_color_class="util-cold"
                elif [[ "$zone" == "hot" ]]; then
                    if safe_compare "$cpu_avg" ">" "$HOT_CPU_MIN"; then cpu_color_class="util-hot"; fi
                    if safe_compare "$mem_avg" ">" "$HOT_MEM_MIN"; then mem_color_class="util-hot"; fi
                fi

                local cpu_avg_fmt=$(printf "%.1f" "$cpu_avg" 2>/dev/null || echo "$cpu_avg")
                local cpu_peak_fmt=$(printf "%.1f" "$cpu_peak" 2>/dev/null || echo "$cpu_peak")
                local mem_avg_fmt=$(printf "%.1f" "$mem_avg" 2>/dev/null || echo "$mem_avg")
                local mem_peak_fmt=$(printf "%.1f" "$mem_peak" 2>/dev/null || echo "$mem_peak")

                local rec_class="recommendation-maintain"
                if [[ "$recommendation" == "downsize" ]]; then rec_class="recommendation-downsize"
                elif [[ "$recommendation" == "upsize" ]]; then rec_class="recommendation-upsize"
                elif [[ "$recommendation" == *"Optimize"* ]]; then rec_class="recommendation-optimize"
                elif [[ "$recommendation" == *"monitor"* || "$recommendation" == *"Review"* ]]; then rec_class="recommendation-monitor"
                fi
                local rec_display=$(echo "$recommendation" | sed 's/-/ /g' | sed 's/\b\(.\)/\U\1/g')

                local cpu_unit="vCPUs"
                if [[ "$cloud_provider" == *"OCI"* ]]; then
                    cpu_unit="OCPUs"
                fi

                local savings_cell=""
                if safe_compare "$monthly_savings" ">" "0"; then
                    savings_cell="<td class=\"savings-positive\">\$$(printf "%.2f" "$monthly_savings")</td>"
                elif safe_compare "$monthly_savings" "<" "0"; then
                    local cost_increase
                    cost_increase=$(echo "$monthly_savings * -1" | bc)
                    savings_cell="<td class=\"savings-negative\">-\$$(printf "%.2f" "$cost_increase")</td>"
                else
                    savings_cell="<td>-</td>"
                fi

                analyzed_rows+="
                    <tr>
                        <td><strong>$hostname</strong></td>
                        <td><span class=\"zone-badge zone-badge-$zone\">$(echo "$zone" | tr '[:lower:]' '[:upper:]')</span></td>
                        <td class=\"util-cell\">
                            <div><span class=\"$cpu_color_class\">${cpu_avg_fmt}%</span><span class=\"util-peak-text\"> / ${cpu_peak_fmt}%</span></div>
                            <div class=\"volatility-text\">CV: ${cpu_cv}%</div>
                        </td>
                        <td class=\"util-cell\">
                            <div><span class=\"$mem_color_class\">${mem_avg_fmt}%</span><span class=\"util-peak-text\"> / ${mem_peak_fmt}%</span></div>
                            <div class=\"volatility-text\">CV: ${mem_cv}%</div>
                        </td>
                        <td class=\"sparkline-cell\">${cpu_sparkline}${mem_sparkline}</td>
                        <td><div><strong>${instance_type}</strong></div><div class=\"instance-details\">${cloud_provider} • ${cpu_count} ${cpu_unit} • ${memory_total}GB RAM</div></td>
                        <td><div class=\"$rec_class\">$rec_display</div></td>
                        ${savings_cell}
                    </tr>"
            fi
        done <<< "$sorted_data"
    fi

    if [[ -s "$stale_file" ]]; then
        stale_count=$(wc -l < "$stale_file" | tr -d ' ')
        while IFS='|' read -r hostname last_backup_date; do
            [[ -z "$hostname" ]] && continue
            local formatted_date
            formatted_date=$(date -d "$last_backup_date" +'%B %d, %Y' 2>/dev/null || echo "$last_backup_date")
            stale_rows+="
                <tr>
                    <td><strong>$hostname</strong></td>
                    <td>$formatted_date</td>
                    <td>Review for decommissioning</td>
                </tr>"
        done < "$stale_file"
    fi

    # --- Build Storage Table ---
    local all_storage_lines=""
    for storage_file in "${TEMP_DIR}"/*_storage.txt; do
        if [[ -f "$storage_file" ]]; then
            local hostname
            hostname=$(basename "$storage_file" _storage.txt)
            while IFS= read -r line; do
                all_storage_lines+="${hostname}|${line}"$'\n'
            done < "$storage_file"
        fi
    done

    if [[ -n "$all_storage_lines" ]]; then
        local sorted_storage_data
        sorted_storage_data=$(echo -e "$all_storage_lines" | sort -t'|' -k1,1)

        local final_storage_rows_array=()
        while IFS='|' read -r hostname mount_point size_raw used_raw percent_raw; do
            if [[ -z "$hostname" ]]; then
                continue
            fi

            local provisioned_gb
            provisioned_gb=$(convert_to_gb "$size_raw")

            if safe_compare "$provisioned_gb" "<" "200"; then
                continue
            fi

            local used_gb
            used_gb=$(convert_to_gb "$used_raw")
            local used_percent="${percent_raw//%/}"

            local status_class=""
            local status_text=""

            if safe_compare "$used_percent" ">" "85"; then
                status_class="status-hot"
                status_text="Nearing Capacity"
            elif safe_compare "$used_percent" "<" "50"; then
                status_class="status-cold"
                status_text="Underutilized"
                underutilized_storage_count=$((underutilized_storage_count + 1))
            else
                # Skip disks in the nominal range
                continue
            fi

            local row
            row="
                <tr>
                    <td>$hostname</td>
                    <td>$mount_point</td>
                    <td>$(printf "%.1f" "$provisioned_gb")</td>
                    <td>$(printf "%.1f" "$used_gb")</td>
                    <td>${used_percent}%</td>
                    <td><span class=\"$status_class\">$status_text</span></td>
                </tr>"

            final_storage_rows_array+=("$row")
         done <<< "$sorted_storage_data"

         storage_rows_count=${#final_storage_rows_array[@]}
         for row in "${final_storage_rows_array[@]}"; do
             storage_table_rows+="$row"
         done
    fi

    # --- Calculate Fleet Efficiency & Trend ---
    if [[ $analyzed_rows_count -gt 0 ]]; then
        efficiency_score=$(awk -v normal="$optimal_count" -v total="$analyzed_rows_count" 'BEGIN { printf "%.1f%%", (normal/total)*100 }')
    fi

    local monthly_trend_html=""
    if [[ -s "$HISTORICAL_SUMMARY_FILE" ]]; then
        local thirty_day_efficiency=$(grep '^MONTHLY_EFFICIENCY' "$HISTORICAL_SUMMARY_FILE" | awk -F'|' '{print $2}')
        if [[ -n "$thirty_day_efficiency" ]] && safe_compare "$thirty_day_efficiency" ">=" "0"; then
            local current_eff_val=$(echo "$efficiency_score" | tr -d '%')
            local trend_arrow="&rarr;"
            local color="#4A5568" # Grey for neutral
            if safe_compare "$current_eff_val" ">" "$thirty_day_efficiency"; then
                trend_arrow="▲"
                color="#059669" # Green
            elif safe_compare "$current_eff_val" "<" "$thirty_day_efficiency"; then
                trend_arrow="▼"
                color="#DC2626" # Red
            fi
            monthly_trend_html="<div class='trend-kpi-container'><span class='trend-kpi-value' style='color:${color};'>${thirty_day_efficiency}% ${trend_arrow} ${current_eff_val}%</span><span class='trend-kpi-label'>30-Day Efficiency Trend</span></div>"
        fi
    fi


    # --- Build Dynamic Executive Summary ---
    local summary_points=""
    local formatted_total_savings
    formatted_total_savings=$(printf "%.0f" "$total_potential_savings")
    if safe_compare "$total_potential_savings" ">" "0"; then
        summary_points+="<li><strong>Potential Monthly Savings:</strong> A total of <strong>\$${formatted_total_savings}</strong> in potential monthly savings was identified by downsizing Cold systems.</li>"
    fi
    if [[ $hot_count -gt 0 ]]; then
        summary_points+="<li><strong>Performance Risk:</strong> <strong>${hot_count}</strong> systems are <strong>Hot</strong> (overutilized). These systems are at risk of performance degradation and should be prioritized for upsizing.</li>"
    fi
    if [[ $cold_count -gt 0 ]]; then
        summary_points+="<li><strong>Cost Savings (Downsize):</strong> <strong>${cold_count}</strong> systems are <strong>Cold</strong> (underutilized CPU &amp; Memory). These are prime candidates for downsizing to reduce cloud spend.</li>"
    fi
    if [[ $optimize_count -gt 0 ]]; then
        summary_points+="<li><strong>Cost Savings (Optimize):</strong> <strong>${optimize_count}</strong> systems are in the <strong>Optimize</strong> zone, with one underutilized resource. These present an opportunity for targeted rightsizing (e.g., changing instance family).</li>"
    fi
    if [[ $underutilized_storage_count -gt 0 ]]; then
        summary_points+="<li><strong>Storage Optimization:</strong> Found <strong>${underutilized_storage_count}</strong> large filesystems (&gt;200GB) that are significantly underutilized and could be candidates for resizing.</li>"
    fi
    summary_points+="<li><strong>Correctly Sized:</strong> <strong>${optimal_count}</strong> systems are <strong>Optimal</strong>, operating efficiently or exempt from changes due to their small size.</li>"
    if [[ "$args_show_stale" == "true" && $stale_count -gt 0 ]]; then
        summary_points+="<li><strong>Stale Systems:</strong> <strong>${stale_count}</strong> systems have not reported data in over ${STALE_THRESHOLD_DAYS} days and may be candidates for decommissioning.</li>"
    fi


    local report_date=$(date +'%B %d, %Y at %H:%M %Z')
    log "INFO" "Generating HTML report: ${report_file}"

    cat > "$report_file" <<EOF
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>Linux Capacity Planning & System Utilization Report</title>
    <style>
        body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif, 'Apple Color Emoji', 'Segoe UI Emoji', 'Segoe UI Symbol'; margin: 0; padding: 20px; background-color: #f8f9fa; color: #2D3748; }
        .container { max-width: 1600px; margin: 0 auto; background-color: #fff; padding: 40px; border-radius: 16px; box-shadow: 0 10px 30px rgba(0,0,0,0.08); }
        h1 { color: #1a202c; text-align: center; margin-bottom: 10px; font-size: 2.5em; font-weight: 700; }
        h2 { color: #1a202c; font-size: 1.8em; margin-top: 1.5em; margin-bottom: 1em; border-bottom: 2px solid #E2E8F0; padding-bottom: 8px; }
        .subtitle { text-align: center; color: #718096; margin-bottom: 40px; font-size: 1.2em; }

        .executive-summary { padding: 25px; background-color: #F7FAFC; border-left: 5px solid #4A5568; border-radius: 8px; margin-bottom: 30px; font-size: 1.1em; line-height: 1.7; }
        .executive-summary h3 { margin-top: 0; border-bottom: none; font-size: 1.2em; text-transform: uppercase; color: #4A5568; letter-spacing: .5px;}
        .executive-summary ul { padding-left: 20px; list-style-position: outside; }
        .executive-summary li { margin-bottom: 10px; }

        .summary-dashboard { display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 20px; }
        .metric-card { background: #fff; border-radius: 12px; padding: 25px; text-align: center; box-shadow: 0 4px 12px rgba(0,0,0,0.06); border: 1px solid #E2E8F0; transition: transform 0.2s, box-shadow 0.2s; }
        .metric-card:hover { transform: translateY(-5px); box-shadow: 0 12px 20px rgba(0,0,0,0.1); }
        .metric-value { font-size: 2.8em; font-weight: 700; margin: 10px 0; }
        .metric-label { color: #718096; font-size: 1em; text-transform: uppercase; letter-spacing: 0.5px; }

        table { width: 100%; border-collapse: collapse; background: white; }
        th { background: #2D3748; color: white; padding: 15px 12px; text-align: left; font-weight: 600; font-size: 0.95em; white-space: nowrap; }
        td { padding: 14px 12px; border-bottom: 1px solid #E2E8F0; font-size: 0.9em; vertical-align: middle; }
        td strong { color: #1a202c; }

        .instance-details { font-size: 0.9em; color: #718096; margin-top: 4px; }

        .zone-badge { display: inline-block; padding: 6px 16px; border-radius: 20px; font-weight: 600; font-size: 0.9em; text-align: center; min-width: 80px; }
        .zone-badge-cold { background-color: #DBEAFE; color: #1E40AF; }
        .zone-badge-optimal { background-color: #D1FAE5; color: #065F46; }
        .zone-badge-hot { background-color: #FEE2E2; color: #991B1B; }
        .zone-badge-optimize { background-color: #FEF3C7; color: #92400E; }
        .zone-badge-unknown { background: #F3F4F6; color: #4B5563; }

        .recommendation-downsize, .recommendation-upsize, .recommendation-maintain, .recommendation-monitor, .recommendation-optimize { font-weight: 600; }
        .recommendation-downsize { color: #2563EB; }
        .recommendation-upsize { color: #DC2626; }
        .recommendation-maintain { color: #059669; }
        .recommendation-monitor, .recommendation-optimize { color: #D97706; }

        .util-cell { line-height: 1.5; }
        .volatility-text { font-size: 0.85em; color: #718096; }
        .util-peak-text { color: #718096; font-size: 0.9em; }

        .sparkline-cell svg { display: block; }
        .savings-positive { color: #059669; font-weight: 600; }
        .savings-negative { color: #DC2626; font-weight: 600; }

        #search-box { width: 100%; box-sizing: border-box; padding: 10px; font-size: 1em; border-radius: 6px; border: 1px solid #CBD5E0; margin-bottom: 20px; }

        .footer { text-align: center; margin-top: 50px; padding-top: 30px; border-top: 1px solid #E2E8F0; color: #718096; font-size: 0.9em; line-height: 1.6; }

        /* Tab Styles */
        .tab-bar { background-color: #f3f4f6; padding: 8px; border-radius: 12px; margin-bottom: 30px; display: flex; justify-content: flex-start; }
        .tab-button { background: transparent; border: none; padding: 12px 25px; font-size: 1.1em; font-weight: 600; cursor: pointer; color: #4b5563; border-radius: 8px; margin-right: 8px; transition: background-color 0.2s, color 0.2s, box-shadow 0.2s; }
        .tab-button.active { background-color: #3b82f6; color: #ffffff; box-shadow: 0 4px 6px rgba(59,130,246,0.25); }
        .tab-button:hover:not(.active) { background-color: #e5e7eb; }
        .tab-panel { display: none; }
        .tab-panel.active { display: block; }
        .section-description { padding: 15px; color: #4A5568; background-color: #F7FAFC; margin: 0 0 20px 0; border: 1px solid #E2E8F0; border-radius: 8px; line-height: 1.6; }

        /* Accordion for Zone Definitions */
        .accordion { background-color: #fff; color: #2D3748; cursor: pointer; padding: 20px; width: 100%; border: 1px solid #E2E8F0; border-radius: 8px; text-align: left; outline: none; font-size: 1.2em; font-weight: 600; transition: 0.4s; margin-top: 30px; }
        .accordion:hover { background-color: #F7FAFC; }
        .accordion:after { content: '+'; font-size: 1.2em; color: #718096; float: right; margin-left: 5px; transition: transform 0.2s; }
        .accordion.active:after { content: "−"; }
        .panel { padding: 0 18px; background-color: white; max-height: 0; overflow: hidden; transition: max-height 0.3s ease-out; border: 1px solid #E2E8F0; border-top: none; border-radius: 0 0 8px 8px; }

        /* Zone Definition Card Styles */
        .zones-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 20px; padding: 25px 5px; }
        .zone-card { border-radius: 12px; padding: 25px; color: #fff; display: flex; flex-direction: column; text-align: center; border: none; }
        .zone-cold { background: #3B82F6; }
        .zone-optimal { background: #10B981; }
        .zone-hot { background: #EF4444; }
        .zone-optimize { background: #F59E0B; color: #382802; }
        .zone-icon { font-size: 2.5em; margin-bottom: 15px; text-shadow: 0 2px 4px rgba(0,0,0,0.2); }
        .zone-name { font-size: 1.5em; font-weight: 600; }
        .zone-count { font-size: 2.8em; font-weight: 700; margin: 5px 0; }
        .zone-definition { font-size: 0.95em; opacity: 0.95; margin-top: 15px; text-align: left; line-height: 1.6; }
        .zone-definition strong { font-weight: 700; }

        /* Trend Analysis Styles */
        .trend-kpi-container { text-align: center; padding: 20px; background: #F7FAFC; border-radius: 8px; margin: 0 0 30px 0; border: 1px solid #E2E8F0;}
        .trend-kpi-value { font-size: 2.5em; font-weight: 700; display: block; }
        .trend-kpi-label { font-size: 1.1em; color: #718096; text-transform: uppercase; letter-spacing: 1px; }
    </style>
</head>
<body>
    <div class="container">
        <h1>🌡️ Linux Capacity Planning & System Utilization Report</h1>
        <p class="subtitle">Generated: ${report_date}</p>

        <div class="tab-bar">
            <button class="tab-button" onclick="openTab(event, 'dashboard')">Dashboard</button>
            <button class="tab-button" onclick="openTab(event, 'system-analysis')">System Analysis</button>
            <button class="tab-button" onclick="openTab(event, 'trends')">Trend & Storage</button>
            <button class="tab-button" onclick="openTab(event, 'exceptions')">Exceptions</button>
        </div>

        <div id="dashboard" class="tab-panel">
            <h2>Dashboard</h2>
            <div class="executive-summary">
                <h3>Executive Summary</h3>
                <ul>${summary_points}</ul>
            </div>
            <div class="summary-dashboard">
                <div class="metric-card"><div class="metric-label">Analyzed Systems</div><div class="metric-value" style="color: #1a202c;">${total_hosts}</div></div>
                <div class="metric-card"><div class="metric-label">Action Required</div><div class="metric-value" style="color: #EF4444;">${action_count}</div></div>
                <div class="metric-card"><div class="metric-label">Optimal Systems</div><div class="metric-value" style="color: #10B981;">${optimal_count}</div></div>
                <div class="metric-card"><div class="metric-label">Potential Monthly Savings</div><div class="metric-value" style="color: #2563EB;">\$${formatted_total_savings}</div></div>
                <div class="metric-card"><div class="metric-label">Fleet Efficiency</div><div class="metric-value" style="color: #10B981;">${efficiency_score}</div></div>
            </div>
            <button class="accordion">Understanding the Temperature Zones</button>
            <div class="panel">
                <div class="zones-grid">
                    <div class="zone-card zone-hot">
                        <div class="zone-icon">🔥</div><div class="zone-name">HOT</div><div class="zone-count">${hot_count}</div>
                        <div class="zone-definition">A system is <strong>Hot</strong> if its average CPU is over <strong>${HOT_CPU_MIN}%</strong> OR its average Memory is over <strong>${HOT_MEM_MIN}%</strong>. These systems are at risk of performance issues and should be upsized.</div>
                    </div>
                    <div class="zone-card zone-optimize">
                        <div class="zone-icon">⚖️</div><div class="zone-name">OPTIMIZE</div><div class="zone-count">${optimize_count}</div>
                        <div class="zone-definition">A system is in the <strong>Optimize</strong> zone if one resource (CPU or Memory) is Cold while the other is Normal. This allows for targeted rightsizing, like changing an instance family.</div>
                    </div>
                    <div class="zone-card zone-cold">
                        <div class="zone-icon">❄️</div><div class="zone-name">COLD</div><div class="zone-count">${cold_count}</div>
                        <div class="zone-definition">A system is <strong>Cold</strong> if its average CPU is under <strong>${COLD_CPU_MAX}%</strong> AND its average Memory is under <strong>${COLD_MEM_MAX}%</strong>. These are prime candidates for downsizing to save costs.</div>
                    </div>
                    <div class="zone-card zone-optimal">
                        <div class="zone-icon">✅</div><div class="zone-name">OPTIMAL</div><div class="zone-count">${optimal_count}</div>
                        <div class="zone-definition">A system is <strong>Optimal</strong> if both resources are in the normal range. Systems are also marked Optimal if they are exempt from downsizing (<strong>&le;2 Cores AND &le;8GB RAM</strong>) to prevent impacting small, essential workloads.</div>
                    </div>
                </div>
            </div>
        </div>

        <div id="system-analysis" class="tab-panel">
            <h2>System Analysis Details (${analyzed_rows_count})</h2>
            <p class="section-description">This section lists systems with sufficient performance data for analysis, sorted by zone. Use the search box to filter by any term (e.g., hostname, zone, OCI, Azure).</p>
            <input type="text" id="search-box" onkeyup="filterTable()" placeholder="Filter systems...">
            <table id="analysis-table">
                <thead>
                    <tr>
                        <th>Hostname</th><th>Zone</th><th>CPU (Avg/Peak) &amp; Volatility</th><th>Memory (Avg/Peak) &amp; Volatility</th><th>Trend (CPU/Mem)</th><th>Instance Details</th><th>Recommendation</th><th>Monthly Savings</th>
                    </tr>
                </thead>
                <tbody>${analyzed_rows}</tbody>
            </table>
        </div>

        <div id="trends" class="tab-panel">
            <h2>Fleet Trend Analysis</h2>
            <p class="section-description">This section provides a strategic, long-term view of fleet health by highlighting the monthly efficiency trend and identifying systems with chronic, sustained utilization issues over the past 30 days. These systems represent the most reliable candidates for rightsizing.</p>
            ${monthly_trend_html}
            ${sustained_hot_html:+'
            <div>
                <h2>Consistently Hot Systems (${sustained_hot_count})</h2>
                <table>
                    <thead><tr><th>Hostname</th><th>Instance Type</th><th>Cores</th><th>Memory</th><th>Avg CPU</th><th>Avg Memory</th><th>Monthly Cost Inc.</th></tr></thead>
                    <tbody>'"${sustained_hot_html}"'</tbody>
                </table>
            </div>'}
            ${sustained_cold_html:+'
            <div style="margin-top: 40px;">
                <h2>Consistently Cold Systems (${sustained_cold_count})</h2>
                <table>
                    <thead><tr><th>Hostname</th><th>Instance Type</th><th>Cores</th><th>Memory</th><th>Avg CPU</th><th>Avg Memory</th><th>Monthly Savings</th></tr></thead>
                    <tbody>'"${sustained_cold_html}"'</tbody>
                </table>
            </div>'}

            <h2 style="margin-top: 40px;">Filesystem Capacity Report (${storage_rows_count})</h2>
            <p class="section-description">This table shows non-OS filesystems over 200GB that are either critically underutilized (&lt;50%) or nearing capacity (&gt;85%).</p>
            <table>
                <thead><tr><th>Hostname</th><th>Mount Point</th><th>Provisioned (GB)</th><th>Used (GB)</th><th>Used %</th><th>Status</th></tr></thead>
                <tbody>${storage_table_rows}</tbody>
            </table>
        </div>

        <div id="exceptions" class="tab-panel">
            ${stale_rows:+'
            <div>
                <h2>Stale Systems (${stale_count})</h2>
                <p class="section-description">Systems whose most recent backup is older than the <strong>${STALE_THRESHOLD_DAYS}-day</strong> threshold. These should be investigated or decommissioned.</p>
                <table>
                    <thead><tr><th>Hostname</th><th>Date of Last Backup</th><th>Recommendation</th></tr></thead>
                    <tbody>${stale_rows}</tbody>
                </table>
            </div>'}

            ${no_data_rows:+'
            <div style="margin-top: 40px;">
                <h2>Systems with No Utilization Data (${no_data_count})</h2>
                <p class="section-description">Systems that were processed but had no CPU or memory metrics. This can happen if the <code>sysstat</code> service was not collecting data during the analysis period.</p>
                <table>
                    <thead><tr><th>Hostname</th><th>Instance Type</th><th>Cloud Provider</th><th>vCPUs</th><th>Memory</th></tr></thead>
                    <tbody>${no_data_rows}</tbody>
                </table>
            </div>'}
        </div>

        <div class="footer">
            <p>This analysis is based on performance data collected over the last <strong>${args_days} days</strong>. Rightsizing 'Cold' and 'Hot' systems is a key component of effective cloud financial management (FinOps).</p>
        </div>
    </div>

    <script>
        // Set the first tab as active on page load
        document.addEventListener("DOMContentLoaded", function() {
            document.querySelector(".tab-button").click();
        });
        function openTab(evt, tabName) {
            var i, tabcontent, tablinks;
            tabcontent = document.getElementsByClassName("tab-panel");
            for (i = 0; i < tabcontent.length; i++) {
                tabcontent[i].style.display = "none";
                tabcontent[i].classList.remove("active");
            }
            tablinks = document.getElementsByClassName("tab-button");
            for (i = 0; i < tablinks.length; i++) {
                tablinks[i].classList.remove("active");
            }
            document.getElementById(tabName).style.display = "block";
            document.getElementById(tabName).classList.add("active");
            evt.currentTarget.classList.add("active");
        }

        function filterTable() {
            var input, filter, table, tr, td, i, txtValue;
            input = document.getElementById("search-box");
            filter = input.value.toUpperCase();
            table = document.getElementById("analysis-table");
            tr = table.getElementsByTagName("tr");
            for (i = 1; i < tr.length; i++) { // Start from 1 to skip header row
                tr[i].style.display = "none"; // Hide row by default
                tds = tr[i].getElementsByTagName("td");
                for (var j = 0; j < tds.length; j++) {
                    td = tds[j];
                    if (td) {
                        txtValue = td.textContent || td.innerText;
                        if (txtValue.toUpperCase().indexOf(filter) > -1) {
                            tr[i].style.display = ""; // Show row if match found
                            break;
                        }
                    }
                }
            }
        }

        var acc = document.getElementsByClassName("accordion");
        for (var i = 0; i < acc.length; i++) {
            acc[i].addEventListener("click", function() {
                this.classList.toggle("active");
                var panel = this.nextElementSibling;
                if (panel.style.maxHeight) {
                    panel.style.maxHeight = null;
                } else {
                    panel.style.maxHeight = panel.scrollHeight + "px";
                }
            });
        }
    </script>
</body>
</html>
EOF

    echo "$report_file"
}

# Function to generate the rich HTML email body
generate_email_body() {
    local efficiency_score="$1"
    local summary_points="$2"
    local monthly_trend_html="$3"
    local hot_count="$4"
    local cold_count="$5"
    local optimize_count="$6"
    local optimal_count="$7"
    local email_hook="$8"
    local formatted_total_savings="$9"


    cat <<EOF
<html>
<body style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif; line-height: 1.6; color: #333; background-color: #f8f9fa; margin: 0; padding: 20px;">
    <table width="100%" border="0" cellspacing="0" cellpadding="0">
        <tr>
            <td align="center">
                <table width="800" border="0" cellspacing="0" cellpadding="40" style="background-color: #ffffff; border-radius: 8px; box-shadow: 0 4px 12px rgba(0,0,0,0.08);">
                    <tr>
                        <td>
                            <h2 style="color: #1a202c; font-size: 24px;">Linux Cloud Capacity &amp; Cost Summary</h2>
                            <p style="color: #4A5568; font-size: 16px;">${email_hook}</p>

                            <table width="100%" border="0" cellspacing="0" cellpadding="0" style="margin-top: 30px; margin-bottom: 30px;">
                                <tr>
                                    <td width="60%" valign="top" style="padding-right: 20px;">
                                        <div style="background-color: #F7FAFC; border-left: 4px solid #4A5568; padding: 1px 20px 10px 20px; border-radius: 4px; height: 100%;">
                                            <h3 style="color: #1a202c; margin-top: 20px;">Key Opportunities</h3>
                                            <ul style="padding-left: 20px; margin-bottom: 20px; color: #4A5568;">
                                                ${summary_points}
                                            </ul>
                                        </div>
                                    </td>
                                    <td width="40%" valign="top" style="padding-left: 20px;">
                                        <div style="background-color: #E6F6F0; border: 1px solid #B4E3D1; border-radius: 8px; padding: 20px; text-align: center; margin-bottom: 20px;">
                                            <div style="font-size: 16px; color: #046A45; text-transform: uppercase; letter-spacing: 0.5px;">Fleet Efficiency KPI</div>
                                            <div style="font-size: 48px; font-weight: bold; color: #047857; margin: 10px 0;">${efficiency_score}</div>
                                            <div style="font-size: 14px; color: #046A45;">Percentage of systems in the Optimal zone.</div>
                                        </div>

                                        ${monthly_trend_html}
                                    </td>
                                </tr>
                            </table>

                            <h3 style="color: #1a202c; font-size: 20px; margin-top: 40px;">Key Metrics Breakdown:</h3>
                            <table style="border-collapse: collapse; width: 100%; border: 1px solid #ddd;">
                                <thead style="background-color: #f2f2f2;">
                                    <tr>
                                        <th style="padding: 12px; border: 1px solid #ddd; text-align: left;">Category</th>
                                        <th style="padding: 12px; border: 1px solid #ddd; text-align: left;">Count / Value</th>
                                        <th style="padding: 12px; border: 1px solid #ddd; text-align: left;">Description</th>
                                    </tr>
                                </thead>
                                <tbody>
                                    <tr>
                                        <td style="padding: 12px; border: 1px solid #ddd;">💰 <strong>Potential Savings</strong></td>
                                        <td style="padding: 12px; border: 1px solid #ddd;"><strong style="color: #059669;">\$${formatted_total_savings}</strong></td>
                                        <td style="padding: 12px; border: 1px solid #ddd;">Estimated monthly savings from downsizing.</td>
                                    </tr>
                                    <tr>
                                        <td style="padding: 12px; border: 1px solid #ddd;">🔥 <strong>Hot Systems</strong></td>
                                        <td style="padding: 12px; border: 1px solid #ddd;"><strong style="color: #DC2626;">${hot_count}</strong></td>
                                        <td style="padding: 12px; border: 1px solid #ddd;"><strong>Immediate Review</strong> (Risk of performance issues)</td>
                                    </tr>
                                    <tr>
                                        <td style="padding: 12px; border: 1px solid #ddd;">❄️ <strong>Cold Systems</strong></td>
                                        <td style="padding: 12px; border: 1px solid #ddd;"><strong style="color: #2563EB;">${cold_count}</strong></td>
                                        <td style="padding: 12px; border: 1px solid #ddd;"><strong>Downsize Candidates</strong> (Significant cost savings)</td>
                                    </tr>
                                    <tr>
                                        <td style="padding: 12px; border: 1px solid #ddd;">⚖️ <strong>Optimize Systems</strong></td>
                                        <td style="padding: 12px; border: 1px solid #ddd;"><strong style="color: #D97706;">${optimize_count}</strong></td>
                                        <td style="padding: 12px; border: 1px solid #ddd;"><strong>Targeted Rightsizing</strong> (e.g., change instance family)</td>
                                    </tr>
                                    <tr>
                                        <td style="padding: 12px; border: 1px solid #ddd;">✅ <strong>Optimal Systems</strong></td>
                                        <td style="padding: 12px; border: 1px solid #ddd;"><strong style="color: #059669;">${optimal_count}</strong></td>
                                        <td style="padding: 12px; border: 1px solid #ddd;"><strong>Correctly Sized</strong> (Operating efficiently)</td>
                                    </tr>
                                </tbody>
                            </table>
                            <br>
                            <p style="text-align: center; margin-top: 30px;"><strong>A detailed, interactive report is attached.</strong></p>
                            <p style="font-size: 0.9em; color: #555; text-align: center;">
                                <em>Please Note: For the best experience with interactive features (like search and filtering), please open the attached HTML file on a computer. These features may be limited on mobile email clients due to security restrictions on embedded scripts.</em>
                            </p>
                        </td>
                    </tr>
                </table>
            </td>
        </tr>
    </table>
</body>
</html>
EOF
}

# Function to send email
send_email_report() {
    local report_path="$1"
    local recipient="$2"
    local subject="Weekly Linux Cloud Capacity & Cost Optimization Report"

    # Get counts and other data for the email body
    local hot_count=$(awk -F'|' '$6 == "hot"' "$RESULTS_FILE" | wc -l)
    local cold_count=$(awk -F'|' '$6 == "cold"' "$RESULTS_FILE" | wc -l)
    local optimize_count=$(awk -F'|' '$6 == "optimize"' "$RESULTS_FILE" | wc -l)
    local optimal_count=$(awk -F'|' '$6 == "optimal"' "$RESULTS_FILE" | wc -l)
    local analyzed_count=$((hot_count + cold_count + optimize_count + optimal_count))
    local efficiency_score=$(awk -v normal="$optimal_count" -v total="$analyzed_count" 'BEGIN { if (total > 0) printf "%.1f%%", (normal/total)*100; else print "0.0%" }')

    # Calculate total savings by reading the final results file
    local total_potential_savings=0.00
    if [[ -s "$RESULTS_FILE" ]]; then
        # The 18th field is the monthly_savings
        while IFS='|' read -r -a fields; do
            local savings=${fields[17]}
            if safe_compare "${savings:-0}" ">" "0"; then
                total_potential_savings=$(echo "scale=2; $total_potential_savings + $savings" | bc)
            fi
        done < "$RESULTS_FILE"
    fi
    local formatted_total_savings
    formatted_total_savings=$(printf "%.0f" "$total_potential_savings")


    # Get historical summary data
    local thirty_day_efficiency=0
    if [[ -s "$HISTORICAL_SUMMARY_FILE" ]]; then
        thirty_day_efficiency=$(grep '^MONTHLY_EFFICIENCY' "$HISTORICAL_SUMMARY_FILE" | awk -F'|' '{print $2}')
    fi

    # Build the summary points for the email
    local summary_points=""
    if safe_compare "$formatted_total_savings" ">" "0"; then
        summary_points+="<li>A total of <strong>\$${formatted_total_savings}</strong> in potential monthly savings was identified.</li>"
    fi
    local total_opportunities=$((cold_count + optimize_count))
    if [[ $total_opportunities -gt 0 ]]; then summary_points+="<li><strong>${total_opportunities} systems</strong> present clear opportunities for immediate cost savings through rightsizing.</li>"; fi
    if [[ $hot_count -gt 0 ]]; then summary_points+="<li><strong>${hot_count} systems</strong> are <strong>Hot</strong> and should be reviewed to mitigate performance risks.</li>"; fi
    summary_points+="<li>Our overall fleet efficiency stands at <strong>${efficiency_score}</strong>. The attached report details the path to improving this KPI.</li>"

    # Build the main email hook
    local email_hook="This week's analysis has identified significant, actionable opportunities to reduce cloud spend and improve performance. Key findings include **${cold_count} systems** that are prime candidates for downsizing and **${hot_count} systems** that are running hot, posing a performance risk."
    # Build the HTML for the monthly trend tile
    local monthly_trend_html=""
    if [[ -n "$thirty_day_efficiency" ]] && safe_compare "$thirty_day_efficiency" ">=" "0"; then
        local current_eff_val=$(echo "$efficiency_score" | tr -d '%')
        local trend_arrow="&rarr;"
        local color="#4A5568" # Grey
        if safe_compare "$current_eff_val" ">" "$thirty_day_efficiency"; then
            trend_arrow="▲"
            color="#047857" # Green
        elif safe_compare "$current_eff_val" "<" "$thirty_day_efficiency"; then
            trend_arrow="▼"
            color="#B91C1C" # Red
        fi

        monthly_trend_html="
        <div style=\"background-color: #F8F9FA; border: 1px solid #E2E8F0; border-radius: 8px; padding: 20px; text-align: center;\">
            <div style=\"font-size: 16px; color: #4A5568; text-transform: uppercase; letter-spacing: 0.5px;\">Monthly Efficiency Trend</div>
            <div style=\"font-size: 32px; font-weight: bold; color: ${color}; margin: 10px 0;\">
                ${thirty_day_efficiency}% &nbsp;${trend_arrow}&nbsp; ${current_eff_val}%
            </div>
            <div style=\"font-size: 14px; color: #718096;\">Comparison vs. 30 days ago.</div>
        </div>"
    fi

    local email_body
    email_body=$(generate_email_body "$efficiency_score" "$summary_points" "$monthly_trend_html" "$hot_count" "$cold_count" "$optimize_count" "$optimal_count" "$email_hook" "$formatted_total_savings")

    log "INFO" "Attempting to send email to ${recipient} with report: ${report_path}"

    if ! command -v mailx &>/dev/null; then
        log "ERROR" "'mailx' command not found. Please install it. Cannot send email."
        return 1
    fi

    # Construct the email with correct MIME headers for HTML content and an attachment
    (
        echo "To: ${recipient}"
        echo "Subject: ${subject}"
        echo "MIME-Version: 1.0"
        echo "Content-Type: multipart/mixed; boundary=\"BOUNDARY\""
        echo
        echo "--BOUNDARY"
        echo "Content-Type: text/html; charset=UTF-8"
        echo
        echo "$email_body"
        echo
        echo "--BOUNDARY"
        echo "Content-Type: text/html; name=\"$(basename "$report_path")\""
        echo "Content-Disposition: attachment; filename=\"$(basename "$report_path")\""
        echo "Content-Transfer-Encoding: base64"
        echo
        base64 "$report_path"
        echo
        echo "--BOUNDARY--"
    ) | /usr/sbin/sendmail -t

    if [[ $? -eq 0 ]]; then
        log "INFO" "Email sent successfully to ${recipient}."
    else
        log "ERROR" "Failed to send email to ${recipient}. Check sendmail configuration."
    fi
}


# Function to show usage
show_usage() {
    echo "Usage: $(basename "$0") [--all | --host <hostname>] [--email <recipient(s)>] [--days <num_days>] [--show-stale] [--help]"
    echo ""
    echo "Analyzes system utilization and generates an HTML report."
    echo ""
    echo "Options:"
    echo "  --all                 Analyze all valid host data directories found under ${NFS_MOUNT}."
    echo "  --host <hostname>     Analyze a specific host."
    echo "  --email <recipient(s)>  Send the report. For multiple recipients, use a single, comma-separated string."
    echo "                        e.g., \"user1@example.com,user2@example.com\""
    echo "  --days <num_days>     Number of days of SAR data to analyze (default: 7)."
    echo "  --show-stale          Include the 'Stale Systems' section in the HTML report."
    echo "  --help                Display this help message."
    echo ""
    echo "Example:"
    echo "  $(basename "$0") --all --email admin@example.com"
    echo "  $(basename "$0") --host myserver --days 3"
    echo "  $(basename "$0") --all --email \"user1@example.com,user2@example.com\""
}

# Parse command line arguments
parse_arguments() {
    while [[ "$#" -gt 0 ]]; do
        case "$1" in
            --all) args_all=true ;;
            --host) if [[ -n "$2" ]]; then args_host="$2"; shift; else log "ERROR" "--host requires a hostname."; show_usage; exit 1; fi ;;
            --email) if [[ -n "$2" ]]; then args_email="$2"; shift; else log "ERROR" "--email requires a recipient."; show_usage; exit 1; fi ;;
            --days) if [[ -n "$2" && "$2" =~ ^[0-9]+$ ]]; then args_days="$2"; shift; else log "ERROR" "--days requires a number."; show_usage; exit 1; fi ;;
            --show-stale) args_show_stale=true ;;
            --help) show_usage; exit 0 ;;
            *) log "ERROR" "Unknown parameter: $1"; show_usage; exit 1 ;;
        esac
        shift
    done

    if ! "$args_all" && [[ -z "$args_host" ]]; then
        log "ERROR" "Either --all or --host <hostname> must be specified.";
        show_usage; exit 1;
    fi
    if "$args_all" && [[ -n "$args_host" ]]; then
        log "ERROR" "Cannot specify both --all and --host."; show_usage; exit 1;
    fi
}

# --- Main Logic ---
main() {
    NFS_SERVER="172.31.88.135:/pool1/critfilebackup"

    parse_arguments "$@"
    mount_nfs

    # Load cost and family data
    load_pricing_data "${SCRIPT_DIR}/pricing.csv"
    load_instance_family_data "${SCRIPT_DIR}/instance_families.csv"

    # Create the trends file header if it does not exist
    if [[ ! -f "$TRENDS_FILE" ]]; then
        log "INFO" "Creating new historical trends file: ${TRENDS_FILE}"
        echo "AnalysisDate,Hostname,AvgCPU,PeakCPU,AvgMem,PeakMem,Zone,Recommendation,InstanceType,Platform,CoreCount,MemoryGB" > "$TRENDS_FILE"
    fi

    # Clear previous results and temporary files
    > "$RESULTS_FILE" || { log "ERROR" "Failed to clear results file: $RESULTS_FILE"; exit 1; }
    > "$SKIPPED_FILE" || { log "ERROR" "Failed to clear skipped file: $SKIPPED_FILE"; exit 1; }
    > "$STALE_FILE" || { log "ERROR" "Failed to clear stale file: $STALE_FILE"; exit 1; }

    # Central processing logic to be called for both --all and --host
    process_host_directory() {
        local hostname="$1"
        local host_base_dir="$2"

        log "INFO" "Processing host directory: ${host_base_dir}"

        if ! ls -d "${host_base_dir}"/backup_* 1>/dev/null 2>&1; then
            printf "%s|Not a valid host data directory (no 'backup_*' found)\n" "$hostname" >> "$SKIPPED_FILE"
            return
        fi

        local latest_backup
        latest_backup=$(ls -td "${host_base_dir}"/backup_* 2>/dev/null | head -1)

        if [[ ! -d "$latest_backup" ]]; then
            printf "%s|No backup directories found\n" "${hostname}" >> "$SKIPPED_FILE"
            return
        fi

        # --- STALE HOST DETECTION ---
        local backup_date_str
        backup_date_str=$(basename "$latest_backup" | grep -oP '(?<=backup_)\d{8}')

        if [[ -n "$backup_date_str" ]]; then
            local current_seconds
            local backup_seconds
            local age_days
            current_seconds=$(date +%s)
            backup_seconds=$(date -d "$backup_date_str" +%s)
            age_days=$(( (current_seconds - backup_seconds) / 86400 ))

            if (( age_days > STALE_THRESHOLD_DAYS )); then
                log "WARN" "Host ${hostname} is STALE. Last backup is ${age_days} days old (from ${backup_date_str})."
                printf "%s|%s\n" "$hostname" "$backup_date_str" >> "$STALE_FILE"
                return # Skip analysis for stale hosts
            fi
        else
            log "WARN" "Could not determine backup date for ${hostname} from directory: ${latest_backup}. Will proceed with analysis."
        fi
        # --- END STALE HOST DETECTION ---

        analyze_host "$hostname" "$latest_backup" || true
    }


    if "$args_all"; then
        log "INFO" "Starting analysis for all directories under ${NFS_MOUNT}"

        # Get a list of directories to process for the progress bar
        mapfile -t host_dirs < <(find "${NFS_MOUNT}" -mindepth 1 -maxdepth 1 -type d)
        total_hosts=${#host_dirs[@]}
        processed_count=0

        for host_base_dir in "${host_dirs[@]}"; do
            hostname=$(basename "$host_base_dir")
            processed_count=$((processed_count + 1))
            if [[ "$total_hosts" -gt 0 ]]; then
                draw_progress_bar "$processed_count" "$total_hosts" "$hostname"
            fi

            case "$hostname" in
                logs|oci_storage_dump|backups|capacity_planning|retired_host_archive|archive)
                    printf "%s|Known directory, intentionally skipped\n" "$hostname" >> "$SKIPPED_FILE"
                    continue
                    ;;
            esac

            process_host_directory "$hostname" "$host_base_dir"
        done
        echo # Newline after progress bar finishes
    else
        log "INFO" "Starting analysis for single host: ${args_host}"
        host_base_dir="${NFS_MOUNT}/${args_host}"
        if [[ ! -d "$host_base_dir" ]]; then
            log "ERROR" "No data directory found for host: ${args_host} at path: ${host_base_dir}. Exiting."
            exit 1
        fi
        process_host_directory "${args_host}" "$host_base_dir"
    fi

    if [[ -s "$RESULTS_FILE" || -s "$STALE_FILE" ]]; then
        # Analyze historical data before generating reports
        analyze_historical_trends

        report_file=$(generate_html_report)

        if [[ -n "$args_email" ]]; then
            send_email_report "$report_file" "$args_email"
        fi

        log "INFO" ""
        log "INFO" "=== ANALYSIS SUMMARY ==="
        analyzed_systems=$(grep -v "unknown" "$RESULTS_FILE" | wc -l || echo "0")
        no_data_systems=$(grep -c "unknown" "$RESULTS_FILE" || echo "0")
        cold_systems=$(awk -F'|' '$6 == "cold"' "$RESULTS_FILE" | wc -l)
        optimal_systems=$(awk -F'|' '$6 == "optimal"' "$RESULTS_FILE" | wc -l)
        hot_systems=$(awk -F'|' '$6 == "hot"' "$RESULTS_FILE" | wc -l)
        optimize_systems=$(awk -F'|' '$6 == "optimize"' "$RESULTS_FILE" | wc -l)
        stale_systems=$(grep -c . "$STALE_FILE" || echo "0")

        log "INFO" "Total systems analyzed: ${analyzed_systems}"
        log "INFO" "Hot systems (overutilized): ${hot_systems}"
        log "INFO" "Optimize systems (imbalanced): ${optimize_systems}"
        log "INFO" "Cold systems (underutilized): ${cold_systems}"
        log "INFO" "Optimal systems (right-sized): ${optimal_systems}"
        log "INFO" "Stale systems (inactive): ${stale_systems}"
        log "INFO" "Systems with no data: ${no_data_systems}"
        log "INFO" "========================"

        echo ""
        echo "✅ Report generated successfully: ${report_file}"
        echo ""
        echo "📊 Summary:"
        echo "  ✔️  Analyzed: ${analyzed_systems}"
        echo "  🔥  Hot systems: ${hot_systems}"
        echo "  ⚖️  Optimize systems: ${optimize_systems}"
        echo "  ❄️  Cold systems: ${cold_systems}"
        echo "  ✅  Optimal systems: ${optimal_systems}"
        echo "  🗑️  Stale systems: ${stale_systems}"
        echo "  🟡 No Data: ${no_data_systems}"
        echo ""
        echo "📧 Email: $(if [[ -n "$args_email" ]]; then echo "Sent to $args_email"; else echo "Not requested"; fi)"

    else
        log "WARN" "No analysis results or skipped hosts found. Check input data and logs for errors."
        echo "❌ No data processed. Please check the log file: $LOG_FILE for details."
    fi
}

# Execute main function with all provided script arguments
main "$@"
