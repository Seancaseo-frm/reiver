# Reiver + Okta Integration Guide

Configure Single Sign-On (SSO) and automatic user provisioning (SCIM) for Reiver using Okta.

## Overview

Reiver supports:
- **SSO via OIDC** - Users authenticate through Okta
- **SCIM 2.0 Provisioning** - Automatic user lifecycle management
- **Group-to-Role Mapping** - Okta groups determine Reiver permissions

---

## Prerequisites

- Okta administrator access
- Reiver account with admin privileges
- Reiver Infrastructure Pro or Enterprise plan (for SCIM)

---

## Part 1: Configure Single Sign-On (SSO)

### Step 1: Create an OIDC Application in Okta

1. Log in to your **Okta Admin Console**
2. Navigate to **Applications → Applications**
3. Click **Create App Integration**
4. Select:
   - Sign-in method: **OIDC - OpenID Connect**
   - Application type: **Web Application**
5. Click **Next**

### Step 2: Configure the OIDC Application

| Field | Value |
|-------|-------|
| App integration name | `Reiver` |
| Logo | Upload Reiver logo (optional) |
| Grant type | ✅ Authorization Code |
| Sign-in redirect URIs | `https://your-reiver-domain.com/api/sso/callback/okta` |
| Sign-out redirect URIs | `https://your-reiver-domain.com/logout` |
| Controlled access | Assign to appropriate groups |

Click **Save**.

### Step 3: Note Your Credentials

From the application's **General** tab, copy:
- **Client ID**
- **Client Secret**
- **Okta Domain** (e.g., `your-company.okta.com`)

### Step 4: Configure Reiver

1. Log in to Reiver as an administrator
2. Navigate to **Settings → SSO Configuration**
3. Click **Add SSO Provider**
4. Enter:

| Field | Value |
|-------|-------|
| Provider | `okta` |
| Name | `Okta SSO` |
| Issuer URL | `https://your-company.okta.com` |
| Client ID | (from Step 3) |
| Client Secret | (from Step 3) |
| Scopes | `openid`, `profile`, `email` |
| Auto-create users | ✅ Enabled (recommended) |
| Default role | `member` |
| Allowed email domains | `yourcompany.com` (optional) |

5. Click **Save**

### Step 5: Test SSO

1. Open an incognito browser window
2. Navigate to your Reiver login page
3. Click **Sign in with Okta**
4. Authenticate with Okta
5. Verify you're logged into Reiver

---

## Part 2: Configure SCIM Provisioning

SCIM enables automatic user provisioning - when users are added/removed in Okta, they're automatically added/removed in Reiver.

### Step 1: Generate a SCIM Bearer Token in Reiver

1. In Reiver, navigate to **Settings → SSO Configuration**
2. Select your Okta SSO configuration
3. Enable **SCIM Provisioning**
4. Click **Generate SCIM Token**
5. Copy the token (you won't be able to see it again)

### Step 2: Enable SCIM in Okta

1. In Okta Admin Console, go to your Reiver application
2. Navigate to the **Provisioning** tab
3. Click **Configure API Integration**
4. Check **Enable API integration**
5. Enter:

| Field | Value |
|-------|-------|
| Base URL | `https://your-reiver-domain.com/scim/v2` |
| API Token | (SCIM token from Step 1) |

6. Click **Test API Credentials**
7. Verify the test succeeds
8. Click **Save**

### Step 3: Enable Provisioning Features

1. In the **Provisioning** tab, click **To App** in the left sidebar
2. Click **Edit**
3. Enable:
   - ✅ Create Users
   - ✅ Update User Attributes
   - ✅ Deactivate Users
4. Click **Save**

### Step 4: Configure Attribute Mappings

Default mappings should work, but verify:

| Okta Attribute | Reiver Attribute |
|----------------|------------------|
| `userName` | `userName` |
| `givenName` | `name.givenName` |
| `familyName` | `name.familyName` |
| `email` | `emails[primary eq true].value` |
| `displayName` | `displayName` |

---

## Part 3: Configure Group-to-Role Mapping

Map Okta groups to Reiver roles for automatic permission assignment.

### Step 1: Create Groups in Okta

Create groups for each Reiver role:
- `Reiver-Admins` → Full administrative access
- `Reiver-Members` → Standard user access
- `Reiver-Viewers` → Read-only access

### Step 2: Assign Users to Groups

Add users to the appropriate Okta groups.

### Step 3: Configure Group Mappings in Reiver

**Option A: Via Reiver Admin UI**

1. Navigate to **Settings → SSO Configuration → Group Mappings**
2. Add mappings:

| Okta Group | Reiver Role |
|------------|-------------|
| `Reiver-Admins` | `admin` |
| `Reiver-Members` | `member` |
| `Reiver-Viewers` | `viewer` |

**Option B: Via API**

```bash
curl -X POST https://your-reiver-domain.com/scim/v2/GroupMappings \
  -H "Content-Type: application/json" \
  -d '{
    "sso_config_id": "your-sso-config-uuid",
    "external_group_id": "okta-group-id",
    "external_group_name": "Reiver-Admins",
    "reiver_role": "admin"
  }'
```

### Step 4: Push Groups from Okta

1. In Okta, go to your Reiver application
2. Navigate to **Push Groups** tab
3. Click **Push Groups → Find groups by name**
4. Search for your Reiver groups
5. Select and push each group

---

## Troubleshooting

### SSO Login Fails

1. Verify the Issuer URL is correct (no trailing slash)
2. Check that the redirect URI in Okta matches exactly
3. Ensure the user's email domain is in the allowed list (if configured)

### SCIM Provisioning Fails

1. Test API credentials in Okta's provisioning settings
2. Check the SCIM token hasn't expired
3. Verify the Base URL is correct: `https://your-domain.com/scim/v2`

### Users Not Getting Correct Roles

1. Verify group mappings are configured
2. Check that users are assigned to the correct Okta groups
3. Ensure groups are pushed to Reiver

### User Deactivation Not Working

1. Verify "Deactivate Users" is enabled in Okta provisioning settings
2. Check that the user was originally provisioned via SCIM

---

## Security Recommendations

1. **Use a service account** for the SCIM API token
2. **Disable SAML JIT provisioning** if using SCIM to avoid conflicts
3. **Restrict allowed email domains** to prevent unauthorized access
4. **Regularly rotate** the SCIM bearer token
5. **Monitor audit logs** for provisioning events

---

## API Reference

### SCIM Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/scim/v2/Users` | GET | List users |
| `/scim/v2/Users` | POST | Create user |
| `/scim/v2/Users/{id}` | GET | Get user |
| `/scim/v2/Users/{id}` | PUT | Replace user |
| `/scim/v2/Users/{id}` | PATCH | Update user |
| `/scim/v2/Users/{id}` | DELETE | Deactivate user |
| `/scim/v2/Groups` | GET | List groups |
| `/scim/v2/Groups/{id}` | GET | Get group |
| `/scim/v2/ServiceProviderConfig` | GET | SCIM capabilities |
| `/scim/v2/Schemas` | GET | SCIM schemas |

### SSO Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/sso/login/okta` | GET | Initiate Okta login |
| `/api/sso/callback/okta` | GET | Handle Okta callback |
| `/api/sso/configurations` | GET/POST | Manage SSO configs |

---

## Support

- Documentation: https://docs.reiver.io
- Email: support@reiver.io
- Status: https://status.reiver.io
