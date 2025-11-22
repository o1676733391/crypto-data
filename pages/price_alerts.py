"""Price alerts configuration and monitoring"""
import streamlit as st
import pandas as pd
from datetime import datetime
from src.config import get_settings
from supabase import create_client

settings = get_settings()


@st.cache_resource
def get_supabase_client():
    """Initialize Supabase client"""
    return create_client(
        str(settings.supabase_url),
        settings.supabase_service_role_key
    )


def create_alerts_table():
    """Create alerts table if it doesn't exist"""
    client = get_supabase_client()
    
    # Note: This is just for documentation - actual table creation should be done via Supabase UI
    # or SQL migration. Supabase Python client doesn't support DDL operations.
    pass


def fetch_alerts():
    """Fetch all price alerts"""
    client = get_supabase_client()
    
    try:
        response = client.table('price_alerts')\
            .select("*")\
            .order("created_at", desc=True)\
            .execute()
        return response.data
    except Exception as e:
        # Table might not exist yet
        st.info("Price alerts table not found. SQL to create it:")
        st.code("""
CREATE TABLE price_alerts (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    symbol VARCHAR(20) NOT NULL,
    condition VARCHAR(10) NOT NULL, -- 'above' or 'below'
    target_price DECIMAL(20, 8) NOT NULL,
    notification_type VARCHAR(20) DEFAULT 'toast', -- 'toast', 'email', 'telegram'
    is_active BOOLEAN DEFAULT true,
    triggered_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX idx_price_alerts_active ON price_alerts(symbol, is_active);
        """, language="sql")
        return []


def create_alert(symbol: str, condition: str, target_price: float, notification_type: str):
    """Create a new price alert"""
    client = get_supabase_client()
    
    try:
        response = client.table('price_alerts').insert({
            'symbol': symbol,
            'condition': condition,
            'target_price': target_price,
            'notification_type': notification_type,
            'is_active': True
        }).execute()
        return True, "Alert created successfully!"
    except Exception as e:
        return False, f"Error creating alert: {str(e)}"


def delete_alert(alert_id: str):
    """Delete a price alert"""
    client = get_supabase_client()
    
    try:
        client.table('price_alerts').delete().eq('id', alert_id).execute()
        return True, "Alert deleted successfully!"
    except Exception as e:
        return False, f"Error deleting alert: {str(e)}"


def toggle_alert(alert_id: str, is_active: bool):
    """Toggle alert active status"""
    client = get_supabase_client()
    
    try:
        client.table('price_alerts')\
            .update({'is_active': is_active})\
            .eq('id', alert_id)\
            .execute()
        return True, f"Alert {'activated' if is_active else 'deactivated'}!"
    except Exception as e:
        return False, f"Error updating alert: {str(e)}"


def render():
    st.title("🔔 Price Alerts")
    st.markdown("Configure price alerts and get notified when targets are reached")
    
    # Create new alert section
    st.subheader("➕ Create New Alert")
    
    with st.form("new_alert_form"):
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            symbol = st.selectbox(
                "Symbol",
                ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT"]
            )
        
        with col2:
            condition = st.selectbox(
                "Condition",
                ["above", "below"]
            )
        
        with col3:
            target_price = st.number_input(
                "Target Price ($)",
                min_value=0.01,
                value=50000.0,
                step=100.0,
                format="%.2f"
            )
        
        with col4:
            notification_type = st.selectbox(
                "Notification",
                ["toast", "email", "telegram"]
            )
        
        submit = st.form_submit_button("Create Alert", use_container_width=True)
        
        if submit:
            success, message = create_alert(symbol, condition, target_price, notification_type)
            if success:
                st.success(message)
                st.rerun()
            else:
                st.error(message)
    
    st.markdown("---")
    
    # Display existing alerts
    st.subheader("📋 Active Alerts")
    
    alerts = fetch_alerts()
    
    if not alerts:
        st.info("No alerts configured yet. Create one above to get started!")
        return
    
    # Convert to DataFrame for display
    df = pd.DataFrame(alerts)
    
    # Display alerts in a nice format
    for idx, alert in enumerate(alerts):
        with st.container():
            col1, col2, col3, col4, col5 = st.columns([2, 2, 2, 2, 1])
            
            with col1:
                status_emoji = "🟢" if alert['is_active'] else "⚪"
                st.markdown(f"**{status_emoji} {alert['symbol']}**")
            
            with col2:
                st.markdown(f"Price **{alert['condition']}** ${alert['target_price']:,.2f}")
            
            with col3:
                st.markdown(f"📢 {alert['notification_type']}")
            
            with col4:
                if alert.get('triggered_at'):
                    st.markdown(f"✅ Triggered at {alert['triggered_at']}")
                else:
                    st.markdown("⏳ Waiting...")
            
            with col5:
                # Action buttons
                if alert['is_active']:
                    if st.button("Pause", key=f"pause_{alert['id']}"):
                        success, msg = toggle_alert(alert['id'], False)
                        if success:
                            st.success(msg)
                            st.rerun()
                else:
                    if st.button("Resume", key=f"resume_{alert['id']}"):
                        success, msg = toggle_alert(alert['id'], True)
                        if success:
                            st.success(msg)
                            st.rerun()
                
                if st.button("🗑️", key=f"delete_{alert['id']}"):
                    success, msg = delete_alert(alert['id'])
                    if success:
                        st.success(msg)
                        st.rerun()
                    else:
                        st.error(msg)
            
            st.markdown("---")
    
    # Alert statistics
    st.subheader("📊 Alert Statistics")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Total Alerts", len(alerts))
    
    with col2:
        active_count = sum(1 for a in alerts if a['is_active'])
        st.metric("Active", active_count)
    
    with col3:
        triggered_count = sum(1 for a in alerts if a.get('triggered_at'))
        st.metric("Triggered", triggered_count)
    
    with col4:
        waiting_count = sum(1 for a in alerts if a['is_active'] and not a.get('triggered_at'))
        st.metric("Waiting", waiting_count)
    
    # Instructions
    with st.expander("ℹ️ How Alerts Work"):
        st.markdown("""
        ### Alert Types
        
        - **Above**: Trigger when price goes above target
        - **Below**: Trigger when price goes below target
        
        ### Notification Methods
        
        - **Toast**: Browser notification (visible in app)
        - **Email**: Send email notification (requires SMTP setup)
        - **Telegram**: Send Telegram message (requires bot setup)
        
        ### Notes
        
        - Alerts check every 5 seconds (matches data ingestion interval)
        - Once triggered, alerts remain active for monitoring
        - You can pause/resume alerts anytime
        - Delete alerts you no longer need
        """)
