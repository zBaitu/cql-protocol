pub fn auth_response_token(user: &str, password: &str) -> Vec<u8> {
    let user_bytes = user.as_bytes();
    let password_bytes = password.as_bytes();
    let mut v: Vec<u8> = Vec::with_capacity(2 + user_bytes.len() + password_bytes.len());
    v.push(0);
    v.extend_from_slice(user_bytes);
    v.push(0);
    v.extend_from_slice(password_bytes);
    v
}

