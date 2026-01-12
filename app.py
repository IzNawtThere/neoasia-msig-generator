"""
MGIS Insurance Declaration Generator - Streamlit UI

This is the presentation layer only. All business logic is in the pipeline module.

Design Principles:
1. UI code has no business logic
2. All state managed through pipeline and session_state
3. Progress callbacks enable responsive UI
4. Error handling shows user-friendly messages
"""

import streamlit as st
import pandas as pd
import tempfile
import os
from datetime import date
from typing import Optional

# Import pipeline components
from config.settings import Settings
from pipeline import ProcessingPipeline, ProcessingProgress
from models.shipment import (
    InboundShipment, OutboundShipment, TransportMode,
    ValidationSeverity
)

# ============================================================================
# Page Configuration
# ============================================================================

st.set_page_config(
    page_title="MGIS Insurance Declaration Generator",
    page_icon="📦",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ============================================================================
# Session State Initialization
# ============================================================================

def init_session_state():
    """Initialize session state variables"""
    if 'pipeline' not in st.session_state:
        st.session_state.pipeline = None
    if 'settings' not in st.session_state:
        st.session_state.settings = None
    if 'processed' not in st.session_state:
        st.session_state.processed = False

init_session_state()

# ============================================================================
# Helper Functions
# ============================================================================

def get_pipeline() -> Optional[ProcessingPipeline]:
    """Get or create pipeline instance"""
    if st.session_state.pipeline is None:
        if st.session_state.settings is not None:
            st.session_state.pipeline = ProcessingPipeline(st.session_state.settings)
    return st.session_state.pipeline


def shipments_to_dataframe(shipments, shipment_type: str) -> pd.DataFrame:
    """Convert shipment list to editable DataFrame with status indicators"""
    if shipment_type == "inbound":
        records = []
        for s in shipments:
            # Determine if SAP data is linked
            has_sap_data = bool(s.currency and s.total_value)
            status = "✅" if has_sap_data else "⚠️"
            
            record = {
                '⚡': status,  # Status indicator
                'Reference': s.reference,
                'Date': s.etd_date,
                'Tracking/AWB': s.tracking_or_awb,
                'Incoterms': s.incoterms,
                'Mode': s.mode.value if s.mode else '',
                'Flight/Vessel': s.flight_vessel,
                'From': s.origin_country,
                'To': s.destination_country,
                'Brand': s.get_brand_string(),
                'Currency': s.currency,
                'Total Value': s.total_value,
            }
            # Add country splits
            for country in ['SIN', 'MAL', 'VIT', 'Indonesia', 'PH']:
                record[country] = s.country_splits.get(country, '')
            records.append(record)
        return pd.DataFrame(records)
    
    else:  # outbound
        records = []
        for s in shipments:
            # Check completeness
            is_complete = bool(s.date and s.flight_vehicle and s.destination)
            status = "✅" if is_complete else "⚠️"
            
            records.append({
                '⚡': status,  # Status indicator
                'Invoice': s.invoice_number,
                'Date': s.date,
                'Flight/Vehicle': s.flight_vehicle,
                'Mode': s.mode.value if s.mode else '',
                'From': s.origin,
                'Destination': s.destination,
                'Description': s.description,
                'Currency': s.currency,
                'Value': s.value
            })
        return pd.DataFrame(records)


def dataframe_to_shipments(df: pd.DataFrame, original_shipments, shipment_type: str):
    """Update shipments from edited DataFrame"""
    pipeline = get_pipeline()
    if not pipeline:
        return
    
    if shipment_type == "inbound":
        for idx, row in df.iterrows():
            if idx < len(original_shipments):
                updates = {
                    'etd_date': row.get('Date'),
                    'tracking_or_awb': row.get('Tracking/AWB'),
                    'incoterms': row.get('Incoterms'),
                    'mode': TransportMode.from_string(row.get('Mode', '')),
                    'flight_vessel': row.get('Flight/Vessel'),
                    'origin_country': row.get('From'),
                }
                pipeline.update_inbound_shipment(idx, updates)
    else:
        for idx, row in df.iterrows():
            if idx < len(original_shipments):
                updates = {
                    'date': row.get('Date'),
                    'flight_vehicle': row.get('Flight/Vehicle'),
                    'destination': row.get('Destination'),
                    'description': row.get('Description'),
                    'currency': row.get('Currency'),
                    'value': row.get('Value'),
                }
                pipeline.update_outbound_shipment(idx, updates)


# ============================================================================
# Sidebar
# ============================================================================

def render_sidebar():
    """Render sidebar configuration"""
    with st.sidebar:
        st.header("⚙️ Configuration")
        
        # API Key
        api_key = st.text_input(
            "Claude API Key",
            type="password",
            value=st.session_state.get('api_key', ''),
            help="Enter your Anthropic API key"
        )
        
        if api_key != st.session_state.get('api_key', ''):
            st.session_state.api_key = api_key
            # Recreate settings and pipeline with new key
            st.session_state.settings = Settings.load(api_key=api_key)
            st.session_state.pipeline = None
        
        st.markdown("---")
        
        # Declaration Period
        declaration_period = st.text_input(
            "Declaration Period",
            value=st.session_state.get('declaration_period', 'October-25'),
            help="e.g., September-25, October-25"
        )
        st.session_state.declaration_period = declaration_period
        
        st.markdown("---")
        
        # Info
        st.markdown("### ℹ️ Processing Info")
        st.info(
            f"**Rate Limit Delay:** 10 seconds between API calls\n\n"
            f"This prevents hitting Claude's rate limits."
        )
        
        # Stats
        pipeline = get_pipeline()
        if pipeline:
            result = pipeline.get_result()
            st.markdown("### 📊 Current Session")
            st.metric("SAP PDOs Loaded", len(result.sap_data))
            st.metric("Inbound Records", len(result.inbound_shipments))
            st.metric("Outbound Records", len(result.outbound_shipments))
        
        st.markdown("---")
        
        # Reset button
        if st.button("🔄 Reset Session", type="secondary"):
            st.session_state.pipeline = None
            st.session_state.processed = False
            st.rerun()


# ============================================================================
# Main Content
# ============================================================================

def render_upload_tab():
    """Render the document upload tab"""
    st.header("📤 Upload Documents")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("SAP Export Files")
        sap_files = st.file_uploader(
            "Upload SAP Export Excel files",
            type=['xlsx', 'xls'],
            accept_multiple_files=True,
            key="sap_upload",
            help="Excel files exported from SAP with PDO data"
        )
        
        if sap_files:
            st.success(f"✅ {len(sap_files)} file(s) ready")
            for f in sap_files:
                st.caption(f"  📄 {f.name}")
        
        st.subheader("Inbound PDO PDFs")
        inbound_pdfs = st.file_uploader(
            "Upload Inbound PDO package PDFs",
            type=['pdf'],
            accept_multiple_files=True,
            key="inbound_upload",
            help="PDF packages containing shipping documents"
        )
        
        if inbound_pdfs:
            st.success(f"✅ {len(inbound_pdfs)} file(s) ready")
    
    with col2:
        st.subheader("Outbound AWB PDFs")
        outbound_awb = st.file_uploader(
            "Upload Outbound Air Waybill PDFs",
            type=['pdf'],
            accept_multiple_files=True,
            key="outbound_awb_upload"
        )
        
        if outbound_awb:
            st.success(f"✅ {len(outbound_awb)} file(s) ready")
        
        st.subheader("Outbound Invoice PDFs")
        outbound_inv = st.file_uploader(
            "Upload Outbound Invoice PDFs",
            type=['pdf'],
            accept_multiple_files=True,
            key="outbound_inv_upload"
        )
        
        if outbound_inv:
            st.success(f"✅ {len(outbound_inv)} file(s) ready")
    
    # Store in session state
    st.session_state.sap_files = sap_files
    st.session_state.inbound_pdfs = inbound_pdfs
    st.session_state.outbound_awb = outbound_awb
    st.session_state.outbound_inv = outbound_inv


def render_process_tab():
    """Render the processing tab"""
    st.header("🔄 Process Documents")
    
    pipeline = get_pipeline()
    
    if not st.session_state.get('api_key'):
        st.error("⚠️ Please enter your Claude API key in the sidebar.")
        return
    
    if pipeline is None:
        st.session_state.settings = Settings.load(api_key=st.session_state.api_key)
        pipeline = ProcessingPipeline(st.session_state.settings)
        st.session_state.pipeline = pipeline
    
    # Stage 1: SAP Parsing
    st.subheader("1️⃣ Parse SAP Files")
    
    sap_files = st.session_state.get('sap_files', [])
    
    if st.button("Parse SAP Files", disabled=not sap_files):
        if sap_files:
            with st.spinner("Parsing SAP files..."):
                sap_data = pipeline.load_sap_files(sap_files)
            
            st.success(f"✅ Parsed {len(sap_data)} PDO sheet(s)")
            
            for pdo_name, data in sap_data.items():
                with st.expander(f"📄 {pdo_name}: {data.currency} {data.total_value:,.2f}"):
                    st.write(f"**Brands:** {', '.join(data.brands)}")
                    st.write(f"**Rows:** {data.row_count}")
                    if data.country_splits:
                        st.write("**Country Splits:**")
                        for country, value in data.country_splits.items():
                            st.write(f"  - {country}: {value:,.2f}")
    
    st.markdown("---")
    
    # Stage 2: Inbound Processing
    st.subheader("2️⃣ Extract from Inbound PDFs")
    
    inbound_pdfs = st.session_state.get('inbound_pdfs', [])
    
    if st.button("Process Inbound PDFs", disabled=not inbound_pdfs):
        if inbound_pdfs:
            # Create temp files
            temp_dir = tempfile.mkdtemp()
            pdf_infos = []
            
            for pdf in inbound_pdfs:
                temp_path = os.path.join(temp_dir, pdf.name)
                with open(temp_path, 'wb') as f:
                    f.write(pdf.read())
                pdf_infos.append({'name': pdf.name, 'path': temp_path})
            
            # Progress display
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            def update_progress(progress: ProcessingProgress):
                pct = progress.progress_percent / 100
                progress_bar.progress(pct)
                status_text.text(f"Processing: {progress.current_item} ({progress.items_processed}/{progress.total_items})")
            
            shipments = pipeline.process_inbound_pdfs(pdf_infos, update_progress)
            
            status_text.text(f"✅ Processed {len(shipments)} inbound shipment(s)")
            st.session_state.processed = True
    
    st.markdown("---")
    
    # Stage 3: Outbound Processing
    st.subheader("3️⃣ Extract from Outbound Documents")
    
    awb_files = st.session_state.get('outbound_awb', [])
    inv_files = st.session_state.get('outbound_inv', [])
    
    if st.button("Process Outbound Documents", disabled=not (awb_files or inv_files)):
        temp_dir = tempfile.mkdtemp()
        
        awb_infos = []
        for f in awb_files or []:
            temp_path = os.path.join(temp_dir, f.name)
            with open(temp_path, 'wb') as tf:
                tf.write(f.read())
            awb_infos.append({'name': f.name, 'path': temp_path})
        
        inv_infos = []
        for f in inv_files or []:
            temp_path = os.path.join(temp_dir, f.name)
            with open(temp_path, 'wb') as tf:
                tf.write(f.read())
            inv_infos.append({'name': f.name, 'path': temp_path})
        
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        def update_progress(progress: ProcessingProgress):
            pct = progress.progress_percent / 100
            progress_bar.progress(pct)
            status_text.text(f"Processing: {progress.current_item}")
        
        shipments = pipeline.process_outbound_pdfs(awb_infos, inv_infos, update_progress)
        
        status_text.text(f"✅ Processed {len(shipments)} outbound shipment(s)")
        st.session_state.processed = True


def render_review_tab():
    """Render the review and edit tab"""
    st.header("✏️ Review & Edit")
    
    pipeline = get_pipeline()
    if not pipeline:
        st.info("No data to review. Process documents first.")
        return
    
    result = pipeline.get_result()
    
    # Validation
    st.subheader("🔍 Validation")
    if st.button("Run Validation"):
        issues = pipeline.validate_all()
        
        if not issues:
            st.success("✅ All records passed validation")
        else:
            for ref, issue_list in issues.items():
                with st.expander(f"⚠️ {ref} - {len(issue_list)} issue(s)"):
                    for issue in issue_list:
                        icon = "🔴" if issue.severity == ValidationSeverity.ERROR else "🟡"
                        st.write(f"{icon} **{issue.field}:** {issue.message}")
                        if issue.suggestion:
                            st.caption(f"💡 {issue.suggestion}")
    
    # SAP Linkage Status (for inbound)
    if result.inbound_shipments:
        unlinked = [s for s in result.inbound_shipments if not (s.currency and s.total_value)]
        if unlinked:
            st.warning(
                f"⚠️ **{len(unlinked)} inbound record(s) missing SAP data:** "
                f"{', '.join(s.reference for s in unlinked)}. "
                f"Check that the correct SAP exports are uploaded."
            )
    
    st.markdown("---")
    
    # Inbound Editor
    st.subheader("📥 Inbound Records")
    
    if result.inbound_shipments:
        # Show legend
        st.caption("⚡ Status: ✅ = Complete | ⚠️ = Missing SAP data (needs review)")
        
        df_inbound = shipments_to_dataframe(result.inbound_shipments, "inbound")
        
        edited_inbound = st.data_editor(
            df_inbound,
            num_rows="dynamic",
            use_container_width=True,
            key="inbound_editor",
            disabled=['⚡']  # Status column is read-only
        )
        
        if st.button("💾 Save Inbound Changes"):
            dataframe_to_shipments(edited_inbound, result.inbound_shipments, "inbound")
            st.success("Changes saved")
    else:
        st.info("No inbound records. Process inbound PDFs first.")
    
    st.markdown("---")
    
    # Outbound Editor
    st.subheader("📤 Outbound Records")
    
    # Show warning for incomplete outbound records
    if result.outbound_shipments:
        incomplete = [s for s in result.outbound_shipments if not (s.date and s.flight_vehicle)]
        if incomplete:
            st.warning(
                f"⚠️ **{len(incomplete)} outbound record(s) missing data:** "
                f"{', '.join(s.invoice_number for s in incomplete)}. "
                f"Check Date and Flight/Vehicle fields."
            )
        
        # Show legend
        st.caption("⚡ Status: ✅ = Complete | ⚠️ = Missing fields (needs review)")
        
        df_outbound = shipments_to_dataframe(result.outbound_shipments, "outbound")
        
        edited_outbound = st.data_editor(
            df_outbound,
            num_rows="dynamic",
            use_container_width=True,
            key="outbound_editor",
            disabled=['⚡']  # Status column is read-only
        )
        
        if st.button("💾 Save Outbound Changes"):
            dataframe_to_shipments(edited_outbound, result.outbound_shipments, "outbound")
            st.success("Changes saved")
    else:
        st.info("No outbound records. Process outbound documents first.")


def render_export_tab():
    """Render the export tab"""
    st.header("📊 Generate Declaration")
    
    pipeline = get_pipeline()
    if not pipeline:
        st.warning("No data to export. Process documents first.")
        return
    
    result = pipeline.get_result()
    declaration_period = st.session_state.get('declaration_period', 'October-25')
    
    # Summary
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Inbound Records", len(result.inbound_shipments))
    with col2:
        st.metric("Outbound Records", len(result.outbound_shipments))
    with col3:
        st.metric("Processing Time", f"{result.processing_time_seconds:.1f}s")
    
    st.markdown(f"**Declaration Period:** {declaration_period}")
    
    st.markdown("---")
    
    # Generate button
    if st.button("🚀 Generate Excel File", type="primary", 
                 disabled=not (result.inbound_shipments or result.outbound_shipments)):
        
        with st.spinner("Generating Excel file..."):
            excel_bytes = pipeline.generate_excel(declaration_period)
        
        st.success("✅ Excel file generated!")
        
        filename = f"Marine_Ins_Declare_{declaration_period.replace('-', '_')}.xlsx"
        
        st.download_button(
            label="📥 Download Declaration Excel",
            data=excel_bytes,
            file_name=filename,
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
    
    st.markdown("---")
    
    # Audit trail
    st.subheader("📋 Audit Trail")
    
    if st.checkbox("Show Audit Trail"):
        audit_df = pipeline.get_audit_trail()
        if not audit_df.empty:
            st.dataframe(audit_df, use_container_width=True)
            
            csv = audit_df.to_csv(index=False)
            st.download_button(
                "Download Audit Log (CSV)",
                csv,
                "audit_trail.csv",
                "text/csv"
            )
        else:
            st.info("No audit entries yet")


# ============================================================================
# Main App
# ============================================================================

def main():
    st.title("📦 MGIS Insurance Declaration Generator")
    st.markdown("**NeoAsia (S) Pte Ltd** - Business Analytics Team")
    st.markdown("---")
    
    # Sidebar
    render_sidebar()
    
    # Main tabs
    tab1, tab2, tab3, tab4 = st.tabs([
        "📤 Upload",
        "🔄 Process", 
        "✏️ Review",
        "📊 Export"
    ])
    
    with tab1:
        render_upload_tab()
    
    with tab2:
        render_process_tab()
    
    with tab3:
        render_review_tab()
    
    with tab4:
        render_export_tab()


if __name__ == "__main__":
    main()
