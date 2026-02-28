import re

with open('rfp_scraper/app.py', 'r') as f:
    content = f.read()

old_block = """    if st.button("Identify Jurisdictions", key="identify_jurisdictions_btn"):
        if not api_key:
            st.error("DeepSeek API Key is required.")
        elif not target_lg_states:
             st.error("No states found. Please generate states first.")
        else:
            progress_bar_lg = st.progress(0)
            status_text_lg = st.empty()
            total_lg_states = len(target_lg_states)

            for i, state_name in enumerate(target_lg_states):
                status_text_lg.text(f"Mapping Ecosystem for {state_name} ({i+1}/{total_lg_states}). This may take a minute...")

                # Get State ID
                state_row = df_current_states_lg[df_current_states_lg['name'] == state_name]
                if state_row.empty:
                    continue
                state_id = int(state_row.iloc[0]['id'])

                # Call AI Ecosystem Mapper (Replaces old generate_local_jurisdictions call)
                ecosystem = ai_client.generate_state_ecosystem(state_name)

                # Check for empty response (AI failure)
                is_empty = not any(ecosystem.values())
                if is_empty:
                    st.warning(f"Warning: No ecosystem data returned for {state_name}. AI may have timed out or failed.")

                # 1. Save State Agencies (Linked only to state)
                for agency_name in ecosystem.get("state_agencies", []):
                    db.add_agency(state_id=state_id, name=f"{state_name} - {agency_name}", category="state_agency", local_jurisdiction_id=None)

                # 2. Save Counties & Departments
                for county_obj in ecosystem.get("counties", []):
                    name = county_obj.get("name")
                    if not name: continue
                    jur_id = db.append_local_jurisdiction(state_id, name, "county")
                    for dept in county_obj.get("departments", []):
                        db.add_agency(state_id=state_id, name=f"{name} County - {dept}", category="county_agency", local_jurisdiction_id=jur_id)

                # 3. Save Cities & Departments
                for city_obj in ecosystem.get("cities", []):
                    name = city_obj.get("name")
                    if not name: continue
                    jur_id = db.append_local_jurisdiction(state_id, name, "city")
                    for dept in city_obj.get("departments", []):
                        db.add_agency(state_id=state_id, name=f"City of {name} - {dept}", category="city_agency", local_jurisdiction_id=jur_id)

                # 4. Save Towns & Departments
                for town_obj in ecosystem.get("towns", []):
                    name = town_obj.get("name")
                    if not name: continue
                    jur_id = db.append_local_jurisdiction(state_id, name, "town")
                    for dept in town_obj.get("departments", []):
                        db.add_agency(state_id=state_id, name=f"Town of {name} - {dept}", category="town_agency", local_jurisdiction_id=jur_id)

                progress_bar_lg.progress((i + 1) / total_lg_states)

            # Crucial: Invalidate Cache so the UI instantly reflects the new database writes
            get_cached_local_govs.clear()
            get_cached_agencies.clear()

            status_text_lg.success("Ecosystem Mapping Complete!")
            time.sleep(1)
            st.rerun()"""

new_block = """    if st.button("Identify Jurisdictions", key="identify_jurisdictions_btn"):
        if not api_key:
            st.error("DeepSeek API Key is required.")
        elif not target_lg_states:
             st.error("No states found. Please generate states first.")
        else:
            progress_bar_lg = st.progress(0)
            status_text_lg = st.empty()
            total_lg_states = len(target_lg_states)

            # Pre-load CISA Data to prevent lag during the loop
            cisa_manager = CisaManager()
            cisa_manager._load_data()

            for i, state_name in enumerate(target_lg_states):
                status_text_lg.text(f"Mapping Ecosystem for {state_name} ({i+1}/{total_lg_states}). This may take a minute...")

                state_row = df_current_states_lg[df_current_states_lg['name'] == state_name]
                if state_row.empty:
                    continue
                state_id = int(state_row.iloc[0]['id'])

                state_abbr = get_state_abbreviation(state_name)

                # Call AI Ecosystem Mapper
                ecosystem = ai_client.generate_state_ecosystem(state_name)

                # UI Warning if AI fails completely
                if not any(ecosystem.values()):
                    st.warning(f"⚠️ AI returned no data for {state_name}. You may need to retry.")
                    continue

                # 1. Save State Agencies
                for agency_name in ecosystem.get("state_agencies", []):
                    cisa_url = cisa_manager.get_agency_url(agency_name, state_abbr)
                    db.add_agency(state_id=state_id, name=f"{state_name} - {agency_name}", url=cisa_url, category="state_agency", local_jurisdiction_id=None)

                # 2. Save Counties & Departments
                for county_obj in ecosystem.get("counties", []):
                    name = county_obj.get("name")
                    if not name: continue
                    jur_id = db.append_local_jurisdiction(state_id, name, "county")

                    # CISA often lists counties as "X County" or just "X"
                    cisa_url = cisa_manager.get_agency_url(f"{name} County", state_abbr) or cisa_manager.get_agency_url(name, state_abbr)

                    for dept in county_obj.get("departments", []):
                        db.add_agency(state_id=state_id, name=f"{name} County - {dept}", url=cisa_url, category="county_agency", local_jurisdiction_id=jur_id)

                # 3. Save Cities & Departments
                for city_obj in ecosystem.get("cities", []):
                    name = city_obj.get("name")
                    if not name: continue
                    jur_id = db.append_local_jurisdiction(state_id, name, "city")
                    cisa_url = cisa_manager.get_agency_url(name, state_abbr)

                    for dept in city_obj.get("departments", []):
                        db.add_agency(state_id=state_id, name=f"City of {name} - {dept}", url=cisa_url, category="city_agency", local_jurisdiction_id=jur_id)

                # 4. Save Towns & Departments
                for town_obj in ecosystem.get("towns", []):
                    name = town_obj.get("name")
                    if not name: continue
                    jur_id = db.append_local_jurisdiction(state_id, name, "town")
                    cisa_url = cisa_manager.get_agency_url(name, state_abbr)

                    for dept in town_obj.get("departments", []):
                        db.add_agency(state_id=state_id, name=f"Town of {name} - {dept}", url=cisa_url, category="town_agency", local_jurisdiction_id=jur_id)

                progress_bar_lg.progress((i + 1) / total_lg_states)

            # Invalidate Cache
            get_cached_local_govs.clear()
            get_cached_agencies.clear()

            status_text_lg.success("Ecosystem Mapping Complete!")
            time.sleep(1)
            st.rerun()"""

if old_block in content:
    content = content.replace(old_block, new_block)
    with open('rfp_scraper/app.py', 'w') as f:
        f.write(content)
    print("Patch successful!")
else:
    print("Could not find the exact block to replace!")
