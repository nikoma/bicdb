class SessionsController < ApplicationController
  skip_before_action :authenticate, only: %i[new create]

  def new
  end

  def create
    if user = User.authenticate_by(email: params[:email], password: params[:password])
      reset_session
      session[:user_id] = user.id
      redirect_to root_path
    else
      flash.now[:alert] = "Email or password was incorrect."
      render :new, status: :unprocessable_entity
    end
  end

  def destroy
    reset_session
    redirect_to new_session_path, status: :see_other
  end
end
