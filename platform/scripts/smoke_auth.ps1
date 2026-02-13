param(
    [string]$BaseUrl = "http://127.0.0.1:8080",
    [string]$AdminEmail = $(if ($env:DB_BOOTSTRAP_ADMIN_EMAIL) { $env:DB_BOOTSTRAP_ADMIN_EMAIL } else { "admin@localhost" }),
    [string]$AdminPassword = $env:DB_BOOTSTRAP_ADMIN_PASSWORD
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Write-Step([string]$Message) {
    Write-Host "[STEP] $Message" -ForegroundColor Cyan
}

function Write-Ok([string]$Message) {
    Write-Host "[OK]   $Message" -ForegroundColor Green
}

function Invoke-Api {
    param(
        [Parameter(Mandatory = $true)]
        [ValidateSet("GET", "POST", "PUT", "PATCH", "DELETE")]
        [string]$Method,
        [Parameter(Mandatory = $true)]
        [string]$Path,
        [Parameter()]
        [AllowNull()]
        [object]$Body,
        [Parameter()]
        [string]$AccessToken,
        [Parameter()]
        [int[]]$ExpectedStatus = @(200)
    )

    $uri = if ($Path.StartsWith("http")) { $Path } else { "$BaseUrl$Path" }
    $headers = @{}
    if ($AccessToken) {
        $headers["Authorization"] = "Bearer $AccessToken"
    }

    $requestArgs = @{
        Method      = $Method
        Uri         = $uri
        ErrorAction = "Stop"
    }
    if ((Get-Command Invoke-WebRequest).Parameters.ContainsKey("UseBasicParsing")) {
        $requestArgs["UseBasicParsing"] = $true
    }
    if ($headers.Count -gt 0) {
        $requestArgs["Headers"] = $headers
    }

    if ($PSBoundParameters.ContainsKey("Body") -and $null -ne $Body) {
        $requestArgs["ContentType"] = "application/json"
        $requestArgs["Body"] = ($Body | ConvertTo-Json -Depth 20 -Compress)
    }

    $statusCode = 0
    $raw = ""
    $response = $null
    $supportsSkipHttpErrorCheck = (Get-Command Invoke-WebRequest).Parameters.ContainsKey("SkipHttpErrorCheck")

    if ($supportsSkipHttpErrorCheck) {
        $requestArgs["SkipHttpErrorCheck"] = $true
        $response = Invoke-WebRequest @requestArgs
        $statusCode = [int]$response.StatusCode
        $raw = if ($null -ne $response.Content) { [string]$response.Content } else { "" }
    }
    else {
        try {
            $response = Invoke-WebRequest @requestArgs
            $statusCode = [int]$response.StatusCode
            $raw = if ($null -ne $response.Content) { [string]$response.Content } else { "" }
        }
        catch {
            $exceptionResponse = $null
            if ($_.Exception -and $_.Exception.PSObject.Properties.Name -contains "Response") {
                $exceptionResponse = $_.Exception.Response
            }
            if ($null -eq $exceptionResponse) {
                $details = if ($null -ne $_.ErrorDetails -and $null -ne $_.ErrorDetails.Message) {
                    [string]$_.ErrorDetails.Message
                } else {
                    [string]$_.Exception.Message
                }
                throw "HTTP request failed for ${Method} ${uri}: $details"
            }
            $statusCode = [int]$exceptionResponse.StatusCode
            $raw = if ($null -ne $_.ErrorDetails -and $null -ne $_.ErrorDetails.Message) {
                [string]$_.ErrorDetails.Message
            } else {
                ""
            }
        }
    }

    if ($ExpectedStatus -notcontains $statusCode) {
        throw "Unexpected HTTP $statusCode for $Method $uri. Body: $raw"
    }

    $data = $null
    if ($raw -and $raw.Trim().Length -gt 0) {
        try {
            if ((Get-Command ConvertFrom-Json).Parameters.ContainsKey("Depth")) {
                $data = $raw | ConvertFrom-Json -Depth 50
            }
            else {
                $data = $raw | ConvertFrom-Json
            }
        }
        catch {
            $data = $raw
        }
    }

    [pscustomobject]@{
        Status = $statusCode
        Data   = $data
        Raw    = $raw
    }
}

if ([string]::IsNullOrWhiteSpace($AdminPassword)) {
    throw "Admin password is required. Set DB_BOOTSTRAP_ADMIN_PASSWORD or pass -AdminPassword."
}

Write-Step "Health check"
$health = Invoke-Api -Method GET -Path "/v1/health" -ExpectedStatus @(200)
Write-Ok "Health endpoint ok"

Write-Step "Admin login"
$adminLogin = Invoke-Api -Method POST -Path "/v1/auth/login" -Body @{
    email    = $AdminEmail
    password = $AdminPassword
} -ExpectedStatus @(200)
if ($null -eq $adminLogin.Data -or -not ($adminLogin.Data.PSObject.Properties.Name -contains "access_token")) {
    throw "Admin login response missing access_token. Raw: $($adminLogin.Raw)"
}
$adminAccess = $adminLogin.Data.access_token
$adminRefresh = $adminLogin.Data.refresh_token
if (-not $adminAccess -or -not $adminRefresh) {
    throw "Admin login did not return access_token and refresh_token."
}
Write-Ok "Admin login ok"

Write-Step "Admin refresh token"
$adminRefreshResp = Invoke-Api -Method POST -Path "/v1/auth/refresh" -Body @{
    refresh_token = $adminRefresh
} -ExpectedStatus @(200)
$adminAccess = $adminRefreshResp.Data.access_token
Write-Ok "Admin refresh ok"

$suffix = [Guid]::NewGuid().ToString("N").Substring(0, 8)
$testEmail = "smoke+$suffix@local.test"
$initialPassword = "Init!${suffix}Aa1"
$resetPassword = "Reset!${suffix}Bb2"
$changedPassword = "Change!${suffix}Cc3"

Write-Step "Create temp trader user (must_reset_password=true)"
$createUserResp = Invoke-Api -Method POST -Path "/v1/users" -AccessToken $adminAccess -Body @{
    email               = $testEmail
    role                = "trader"
    password            = $initialPassword
    must_reset_password = $true
} -ExpectedStatus @(200)
$testUserId = $createUserResp.Data.user.id
if (-not $testUserId) {
    throw "User creation failed: missing user id."
}
Write-Ok "Created test user $testEmail ($testUserId)"

Write-Step "Verify login blocked before reset"
$blockedLogin = Invoke-Api -Method POST -Path "/v1/auth/login" -Body @{
    email    = $testEmail
    password = $initialPassword
} -ExpectedStatus @(403)
Write-Ok "Must-reset login block confirmed"

Write-Step "Issue password reset token"
$issueResetResp = Invoke-Api -Method POST -Path "/v1/users/$testUserId/issue-reset" -AccessToken $adminAccess -ExpectedStatus @(200)
$resetToken = $issueResetResp.Data.reset_token
if (-not $resetToken) {
    throw "Issue-reset failed: missing reset token."
}
Write-Ok "Issued reset token"

Write-Step "Reset password with token"
$resetResp = Invoke-Api -Method POST -Path "/v1/auth/reset-password" -Body @{
    reset_token  = $resetToken
    new_password = $resetPassword
} -ExpectedStatus @(200)
Write-Ok "Password reset ok"

Write-Step "Login with reset password"
$userLogin = Invoke-Api -Method POST -Path "/v1/auth/login" -Body @{
    email    = $testEmail
    password = $resetPassword
} -ExpectedStatus @(200)
$userAccess = $userLogin.Data.access_token
$userRefresh = $userLogin.Data.refresh_token
if (-not $userAccess -or -not $userRefresh) {
    throw "User login after reset missing tokens."
}
Write-Ok "User login after reset ok"

Write-Step "Change password (self-service)"
$changeResp = Invoke-Api -Method POST -Path "/v1/auth/change-password" -AccessToken $userAccess -Body @{
    old_password = $resetPassword
    new_password = $changedPassword
} -ExpectedStatus @(200)
Write-Ok "Password change ok"

Write-Step "Old refresh token must be invalid after password change"
$oldRefreshInvalid = Invoke-Api -Method POST -Path "/v1/auth/refresh" -Body @{
    refresh_token = $userRefresh
} -ExpectedStatus @(401)
Write-Ok "Old refresh token rejected"

Write-Step "Login with changed password"
$userLoginChanged = Invoke-Api -Method POST -Path "/v1/auth/login" -Body @{
    email    = $testEmail
    password = $changedPassword
} -ExpectedStatus @(200)
$userAccessChanged = $userLoginChanged.Data.access_token
$userRefreshChanged = $userLoginChanged.Data.refresh_token
if (-not $userAccessChanged -or -not $userRefreshChanged) {
    throw "User login after password change missing tokens."
}
Write-Ok "User login with changed password ok"

Write-Step "Revoke one refresh token (self)"
$revokeOne = Invoke-Api -Method POST -Path "/v1/auth/revoke" -AccessToken $userAccessChanged -Body @{
    refresh_token = $userRefreshChanged
} -ExpectedStatus @(200)
Write-Ok "Single-token revoke ok"

Write-Step "Revoked refresh token must be invalid"
$revokedInvalid = Invoke-Api -Method POST -Path "/v1/auth/refresh" -Body @{
    refresh_token = $userRefreshChanged
} -ExpectedStatus @(401)
Write-Ok "Revoked refresh token rejected"

Write-Step "Admin revoke-all on temp user"
$revokeAll = Invoke-Api -Method POST -Path "/v1/auth/revoke" -AccessToken $adminAccess -Body @{
    revoke_all = $true
    user_id    = $testUserId
} -ExpectedStatus @(200)
Write-Ok "Admin revoke-all ok"

Write-Host ""
Write-Host "Smoke auth flow completed successfully." -ForegroundColor Green
Write-Host "Temp user email: $testEmail"
Write-Host "Temp user id:    $testUserId"
